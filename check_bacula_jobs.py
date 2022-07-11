#!/usr/local/bin python3

# SELECT * FROM public.client INNER JOIN public.job ON client.clientid = job.clientid WHERE client.name = 'wiki-fd' ORDER BY job.endtime;
from __future__ import annotations
import datetime
import re
import sys
import typing
from typing import AnyStr, Optional, Union, List

import psycopg2

import argparse

if not sys.version_info >= (3,7):
  raise RuntimeError("this script requires Python V3.7 or higher")

totalPerfLabel = "Total OK jobs"

# ...
parser = argparse.ArgumentParser()
parser.add_argument(
    "-H",
    "--host",
    default="localhost",
    help="Bacula database host name or address"
)
parser.add_argument(
    "-p",
    "--port",
    default=5432,
    help="PostgreSQL port (default=5432)"
)
parser.add_argument(
    "-U",
    "--dbuser",
    default="postgres",
    help="Database user (default=potgres)"
)
parser.add_argument(
    "-P",
    "--dbpass",
    help="Database user password"
)
parser.add_argument(
    "-D",
    "--db",
    default="bacula"
)
parser.add_argument(
    "-C",
    "--client",
    required=True,
    help="Bacula client name. "
)
parser.add_argument(
    "-d",
    "--days",
    default=7,
    help="""Number of days to consider when checking the job status for the given client"""
)
parser.add_argument(
    "-j",
    "--job",
    help="""Optional job name"""
)
parser.add_argument(
    "-R",
    "--norunwarn",
    action="store_true",
    help="""No run status gives warning"""
)
parser.add_argument(
    "-w",
    "--warn",
    action="append",
    nargs=2,
    help="""Warning threshold compatible to Nagios threshold range specifica\
            tions. This option takes two arguments, first the target data eithe\
            r '+' for the total successful job count or a job name, and the thr\
            eshold. ex. --warn + 3:"""
)
parser.add_argument(
    "-c",
    "--crit",
    action="append",
    nargs=2,
    help="""Critical threshold compatible to Nagios threshold range specific\
            ations. This option takes two arguments, first the target data eith\
            er '+' for the total successful job count or a job name, and the th\
            reshold. ex. --crit server-backup 3:"""
)

args = parser.parse_args()

POSTGRES_PORT = 5432


###############################################################################
class TRange:

  def __init__(self, min, max, inside: bool = False):
    self.Min = min
    self.Max = max
    self.Inside = inside


def days(d: datetime):
  d = d.date()
  today = datetime.datetime.now().date()
  return (today - d).days


###############################################################################
class TThreshold(TRange):

  # type = TNagios.WARNING  | TNagios.CRITICAL
  def __init__(self, type : int, thresholdStr : AnyStr, target : AnyStr= None, matcher : Optional[typing.Callable[[TThreshold, AnyStr], bool]] = None):
    if type not in [ TNagios.WARNING, TNagios.CRITICAL ]:
      raise RuntimeError("BUG: TThreshold type muts be TNagios.WARNING or TNagios..CRITICAL")
    self.Type = type
    self.Target = target
    self.Matcher = self.defaultMatcher if matcher is None else matcher

    m = re.fullmatch(r"^(\s*[@]?)([-.0-9]*|[~])?(?:([:])([-.0-9]*))?\s*$", thresholdStr)
    if m is None:
      raise RuntimeError(f"Threshold '{thresholdStr}' could not be parsed")
    inside = m.group(1) == '@'
    v1 = m.group(2)
    v2 = m.group(4)

    if (m.group(3) == ':'):
      # We have a range
      if v1 == "": v1 = 0
      v1 = None if v1 == '~' else (float(v1) if '.' in v1 else int(v1))
      v2 = None if v2 == '' else (float(v2) if '.' in v2 else int(v1))
    else:
      # v1 is actually v2 (i.e. :x same as x)
      v2 = None if v1 == '~' else (float(v1) if '.' in v1 else int(v1))
      v1 = 0

    super().__init__(v1, v2, inside)

  #------------------------------------------------------------------------------
  def defaultMatcher(self,thr : TThreshold, lbl : AnyStr):
    return thr.Target == lbl

  # ------------------------------------------------------------------------------
  def __str__(self):
    v1 = '~' if self.Min is None else self.Min
    v2 = '' if self.Max is None else self.Max
    prefix = '@' if self.Inside else ''
    if v1 == 0: return f"{prefix}{v2}"
    return f"{prefix}{v1}:{v2}"


###############################################################################
class TPerfData:
  def __init__(self, label: AnyStr, value: Union[int, float], unit: Optional[AnyStr] = None,
               warn: Optional[TThreshold] = None, crit: Optional[TThreshold] = None,
               min: Optional[Union[int, float]] = None, max: Optional[Union[int, float]] = None):
    self.Label = label
    self.Value = value
    self.Unit = unit
    self.WarnThreshold = warn
    self.CritThreshold = crit
    self.Min = min
    self.Max = max

  def __str__(self):
    s = f"'{self.Label}'={self.Value}"
    if self.Unit is not None: s = s + self.Unit
    meta = [self.WarnThreshold, self.CritThreshold, self.Min, self.Max]
    while len(meta) > 0 and meta[-1] is None: del meta[-1]
    meta = ["" if x is None else str(x) for x in meta]
    if len(meta) > 0: s = s + ';' + ';'.join(meta)
    return s


###############################################################################
class TNagios:
  SUCCESS = 0
  WARNING = 1
  CRITICAL = 2
  UNKNOWN = 3

  STATUS = {SUCCESS: "OK", WARNING: "WARNING", CRITICAL: "CRITICAL", UNKNOWN: "UNKNOWN"}

  def __init__(self):
    self.Status = TNagios.UNKNOWN
    self.Message = "unknown status"
    self.PerfDataList = []
    self.ThresholdList : List[TThreshold] = []

  # ------------------------------------------------------------------------------
  def SetStatus(self, status: int, msg: Optional[AnyStr], append: Optional[AnyStr] = None):
    self.Status = status
    if msg is not None:
      if append is not None and len(self.Mesage) > 0:
        self.Message = self.Message + append + msg
      else:
        self.Message = msg

  # ------------------------------------------------------------------------------
  def ShiftStatus(self, status: int, msg: Optional[AnyStr], keepEqual: bool = False, append: Optional[AnyStr] = None):
    if status > TNagios.UNKNOWN: status = TNagios.UNKNOWN
    if self.Status < TNagios.UNKNOWN and self.Status > status: return
    if self.Status == status and keepEqual and not append: return

    self.Status = status
    if msg is not None:
      if append is not None and len(self.Message) > 0:
        self.Message = self.Message + append + msg
      else:
        self.Message = msg

  # ------------------------------------------------------------------------------
  def ReturnStatus(self, status: int, msg: Optional[AnyStr], append: Optional[AnyStr] = None):
    self.SetStatus(status, msg, append)
    self.ReturnResult()

  # ------------------------------------------------------------------------------
  def ProposeReturnStatus(self, status: int, msg: AnyStr, keepEqual: bool = False, append: Optional[AnyStr] = None):
    self.ShiftStatus(status, msg, keepEqual, append)
    self.ReturnResult()

  # ------------------------------------------------------------------------------
  def ReturnResult(self):
    msg = f"{TNagios.STATUS[self.Status]} - {self.Message}"
    if len(self.PerfDataList) > 0:
      perf = ""
      for p in self.PerfDataList:
        perf = perf + str(p) + " "
      perf = perf[:-1]
      msg = msg + "|" + perf
    print(msg)
    sys.exit(self.Status)

  # ------------------------------------------------------------------------------
  def AddPerf(self, perfData: Union[TPerfData, List[TPerfData]]):
    if isinstance(perfData, list):
      for p in perfData: self.AddPerf(p)
      return

    # Try to resolve for thresholds
    for thr in self.ThresholdList:
      if thr.Matcher(thr, perfData.Label):
        if thr.Type == TNagios.WARNING:
          if perfData.WarnThreshold is None: perfData.WarnThreshold=thr
        elif thr.Type == TNagios.CRITICAL:
          if perfData.CritThreshold is None: perfData.CritThreshold=thr
        else:
          raise RuntimeError("BUG: unexpected threshold type")

    self.PerfDataList.append(perfData)

  # ------------------------------------------------------------------------------
  def AddTheshold(self, thr: Union[TThreshold, List[TThreshold]]):
    if isinstance(thr, list):
      self.ThresholdList += thr
    else:
      self.ThresholdList.append(thr)


Nagios = TNagios()


# dbUser = "monitor"
# dbPass = "ofZT8e4XckW"


class TBacula:

  def __init__(self, dbURI: AnyStr, dbUser: AnyStr, dbPass: AnyStr):
    m = re.fullmatch(r"(?P<host>[^:]+)[:](?P<port>[0-9]*)[/][/](?P<db>.+)", dbURI)
    if m is None:
      Nagios.ReturnStatus(TNagios.CRITICAL, f"invalid db URI: '{dbURI}'")

    self.DBHost = m.group("host")
    self.DBPort = int(m.group("port")) if len(m.group("port")) > 0 else POSTGRES_PORT
    self.DBName = m.group("db")
    self.DBUser = dbUser
    self.DBPass = dbPass
    self._cnx = None

  # ----------------------------------------------------------------------------
  @property
  def DBConnection(self):
    if self._cnx is None:
      self.connect()
    return self._cnx

  # ------------------------------------------------------------------------------
  def connect(self):
    try:
      self._cnx = psycopg2.connect(host=self.DBHost,
                                   database=self.DBName,
                                   user=self.DBUser,
                                   password=self.DBPass,
                                   port=self.DBPort,
                                   connect_timeout=3)
    except Exception as e:
      Nagios.ReturnStatus(TNagios.CRITICAL,
                          f"could not connect to postgresql database '{self.DBName}' @ {self.DBHost}:{self.DBPort}")


###############################################################################
class TJobStatus:
  StatusDB = {
    'C': "Created but not yet running",
    'R': "Running",
    'B': "Blocked",
    'T': "Terminated normally",
    'W': "Terminated normally with warnings",
    'E': "Terminated in Error",
    'e': "Non-fatal error",
    'f': "Fatal error",
    'D': "Verify Differences",
    'A': "Canceled by the user",
    'I': "Incomplete Job",
    'F': "Waiting on the File daemon",
    'S': "Waiting on the Storage daemon",
    'm': "Waiting for a new Volume to be mounted",
    'M': "Waiting for a Mount",
    's': "Waiting for Storage resource",
    'j': "Waiting for Job resource",
    'c': "Waiting for Client resource",
    'd': "Wating for Maximum jobs",
    't': "Waiting for Start Time",
    'p': "Waiting for higher priority job to finish",
    'i': "Doing batch insert file records",
    'a': "SD despooling attributes",
    'l': "Doing data despooling",
    'L': "Committing data (last despool)"
  }

  ShortStatusDB = {
    'C': "created",
    'R': "running",
    'B': "blocked",
    'T': "OK",
    'W': "WARN",
    'E': "ERROR",
    'e': "non-fatal",
    'f': "fatal",
    'D': "verify error",
    'A': "canceled",
    'I': "incomplete",
    'F': "wait FD",
    'S': "wait SD",
    'm': "wait new vol",
    'M': "wait mount",
    's': "wait storage",
    'j': "wait job res.",
    'c': "wait client res.",
    'd': "wait max jobs",
    't': "wait start",
    'p': "wait job",
    'i': "batch file ins.",
    'a': "SD despool attr.",
    'l': "data despool",
    'L': "commit data"
  }

  StatusList = [x for x in StatusDB.keys()]

  StatusGroups = {
    TNagios.SUCCESS : ['T'],
    TNagios.WARNING : ['W', 'D', 'A'],
    TNagios.CRITICAL: ['B', 'E', 'e', 'f', 'I'],
    # We use unkown status for any running status
    TNagios.UNKNOWN : {'R': TNagios.SUCCESS,
                       'F': TNagios.WARNING,
                       'S': TNagios.WARNING,
                       'm': TNagios.WARNING,
                       'M': TNagios.WARNING,
                       's': TNagios.WARNING,
                       'j': TNagios.WARNING,
                       'c': TNagios.WARNING,
                       'i': TNagios.UNKNOWN,
                       'a': TNagios.UNKNOWN,
                       'l': TNagios.UNKNOWN,
                       'L': TNagios.UNKNOWN,
                       'd': TNagios.WARNING,
                       'p': TNagios.WARNING},
    -1              : ['C', 't']  # ignore if just created and not yet run
  }

  SeverityDB = {status: severity for severity, lst in StatusGroups.items() for status in lst}

  def __init__(self, status: str):
    if (not isinstance(status, str)) or len(status) != 1:
      raise RuntimeError(f"expect job status to be single character string, is '{status}'")
    if status not in TJobStatus.StatusList:
      raise RuntimeError(f"unknown job status '{status}'")
    self.Status = status

  # ------------------------------------------------------------------------------
  def __str__(self):
    return TJobStatus.StatusDB[self.Status]

  def GetText(self):
    return str(self)

  def GetShortText(self):
    return TJobStatus.ShortStatusDB[self.Status]

  # ------------------------------------------------------------------------------
  def _is(self, target, norunwarn: bool):
    if self.Status in TJobStatus.StatusGroups[target]: return True
    if norunwarn: return False  # We do not translate some runnign status to other status
    if self.Status not in TJobStatus.StatusGroups[TNagios.UNKNOWN]: return False
    return TJobStatus.StatusGroups[TNagios.UNKNOWN][self.Status] == target

  # ------------------------------------------------------------------------------
  def IsSuccess(self):
    return self._is(TNagios.SUCCESS, args.norunwarn)

  # ------------------------------------------------------------------------------
  def IsWarning(self):
    return self._is(TNagios.WARNING, args.norunwarn)

  # ------------------------------------------------------------------------------
  def IsCritical(self):
    return self._is(TNagios.CRITICAL, args.norunwarn)

  # ------------------------------------------------------------------------------
  def IsRunning(self):
    return self._is(TNagios.UNKNOWN, True)

  # ----------------------------------------------------------------------------
  @property
  def Severity(self):
    s = TJobStatus.SeverityDB[self.Status]
    if s == -1: return TNagios.SUCCESS
    if s != TNagios.UNKNOWN: return s
    if args.norunwarn: return TNagios.SUCCESS
    return TJobStatus.StatusGroups[TNagios.UNKNOWN][self.Status]

  # ------------------------------------------------------------------------------
  def Check():
    for s in TJobStatus.StatusList:
      found = False
      for g in TJobStatus.StatusGroups.values():
        if s in g:
          if found:
            raise RuntimeError(f"BUG: job status '{s}' found in two different groups")
          found = True
      if not found:
        raise RuntimeError(f"BUG: job status '{s}' was not found in any group")


TJobStatus.Check()


###############################################################################
class TClient:

  def __init__(self, bacula: TBacula, clientName: AnyStr, jobName: Optional[AnyStr]):
    self.Bacula = bacula
    self.ClientName = clientName
    self.ClientID = None
    self.JobName = jobName
    self.getClient()
    self.getJobs()

  def getClient(self):
    cursor: psycopg2.cursor = self.Bacula.DBConnection.cursor()
    if self.ClientID is None:
      client = self.ClientName.lower()  # Force lowercase
      clientList = [client]  # Name as given
      if '.' in client: clientList.append(client.split('.')[0])  # Only name in FQDN
      # Now the last entry is the simple client name
      if len(clientList[-1]) > 3 and clientList[-1][-3:] != "-fd": clientList.append(clientList[-1] + "-fd")
      clientList = ["'" + x + "'" for x in clientList]

    sql = f"SELECT client.clientid FROM public.client WHERE lower(client.name) IN ({','.join(clientList)})"
    cursor.execute(sql)
    if cursor.rowcount == 0:
      cursor.close()
      Nagios.ReturnStatus(TNagios.CRITICAL, f"unknown client {','.joint(clientList)}")
    if cursor.rowcount != 1:
      cursor.close()
      Nagios.ReturnStatus(TNagios.WARNING, f"bug: more than one entry found for client {','.joint(clientList)}")
    self.ClientID = cursor.fetchone()[0]
    cursor.close()

  def getJobs(self):
    self.JobList = []
    self.Jobs = {}  # Job list grouped by job name

    cursor: psycopg2.cursor = self.Bacula.DBConnection.cursor()

    # First we select in requested timeframe 'days', if nothing found we query all jobs but limit to given number in order to attempt
    # finding the last executed job
    tod = datetime.datetime.now()
    days = datetime.timedelta(days=args.days)
    sql = f"SELECT name, job, level, jobstatus, jobfiles, jobbytes, schedtime, endtime, realendtime FROM public.job WHERE job.clientid = '{self.ClientID}' AND type = 'B'"
    selection = f"AND realendtime >= CURRENT_DATE - INTERVAL '{int(args.days)} day'"
    sqlPost = f"ORDER BY realendtime DESC"
    selectJob = "" if self.JobName is None else f" AND lower(job.name) = '{self.JobName.lower()}'"
    cursor.execute(f"{sql} {selection}{selectJob} {sqlPost}")

    # No jobs ? Then lets simply take the 20 last backu jobs registered
    if cursor.rowcount == 0:
      # sql = f"SELECT name, job, level, jobstatus, jobfiles, jobbytes, schedtime, endtime, realendtime FROM public.job WHERE job.clientid = '{self.ClientID}' AND type = 'B' ORDER BY realendtime DESC LIMIT 20"
      cursor.execute(f"{sql}{selectJob}{sqlPost} LIMIT 20")

    row = cursor.fetchone()
    while row is not None:
      print(row)
      job = TJob(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[8])
      self.JobList.append(job)
      if job.Name not in self.Jobs: self.Jobs[job.Name] = []
      self.Jobs[job.Name].append(job)
      row = cursor.fetchone()

    cursor.close()
    # SELECT * FROM public.client INNER JOIN public.job ON client.clientid = job.clientid WHERE client.name = 'wiki-fd' ORDER BY job.endtime;

  # ------------------------------------------------------------------------------
  def GetBackupStatus(self):
    if len(self.Jobs) == 0:
      if args.job is None:
        msg = "no backup job found"
      else:
        msg = f"backup job '{args.job}' was not found"
      msg = f"{msg} for client '{self.ClientName}'"
      Nagios.ReturnStatus(TNagios.CRITICAL, msg)

    # List of perfdfata where perfdata[0] is overall job data
    perfData = [TPerfData(totalPerfLabel, 0)]

    Nagios.SetStatus(TNagios.SUCCESS, "")
    for jobName in sorted(self.Jobs.keys()):
      perfData.append(TPerfData(f"{jobName} OK", 0))
      Nagios.ShiftStatus(TNagios.SUCCESS, f"{jobName}:", append=True)
      jobs = self.Jobs[jobName]
      last: TJob = None
      lastSuccess: TJob = None
      lastFullSuccess: TJob = None
      # We expect jobs to be sorted descending by end date (i.e. last job is first in list)
      for j in jobs:
        if last is None: last = j
        if lastSuccess is None and j.Status.IsSuccess(): lastSuccess = j
        if lastFullSuccess is None and j.Level == "F" and j.Status.IsSuccess(): lastFullSuccess = j
        if j.Status.IsSuccess(): perfData[-1].Value += 1
      if last is None:
        # Should never happen
        Nagios.ShiftStatus(TNagios.CRITICAL, "no backup job found", append=True)
        Nagios.ReturnResult()
      else:
        if last.Status.Severity == TNagios.SUCCESS: perfData[0].Value += 1
        Nagios.ShiftStatus(last.Status.Severity,
                           f"Last(level={last.Level}): {last.Status.GetShortText()} {last.EndTime} ({days(lastSuccess.EndTime)} days)",
                           append=' ')
      if lastSuccess is not None and lastSuccess is not last:
        if last.Status.IsRunning(): perfData[0].Value += 1
        Nagios.ShiftStatus(last.Status.Severity,
                           f"Last-OK(level={lastSuccess.Level}): {lastSuccess.EndTime} ({days(lastSuccess.EndTime)} days)",
                           append=', ')
      if lastFullSuccess is not None and lastFullSuccess is not lastSuccess:
        Nagios.ShiftStatus(last.Status.Severity,
                           f"Last-full-OK: {lastFullSuccess.EndTime} ({days(lastFullSuccess.EndTime)} days)",
                           append=', ')

    Nagios.AddPerf(perfData)


###############################################################################
class TJob:

  def __init__(self, name, jobName, level, status, files, bytes, schedTime, endTime):
    self.Name = name  # General job name
    self.JobName = jobName  # Unique scheduled job name
    self.Level = level
    self.Status = TJobStatus(status)
    self.Files = files
    self.Bytes = bytes
    self.ScheduledTime = schedTime
    self.EndTime = endTime


# t1=TThreshold("10")
# t2=TThreshold("10:")
# t3=TThreshold("~:10")
# t4=TThreshold(":10")
# t5=TThreshold("10:20")
# t6=TThreshold("@10:20")

#------------------------------------------------------------------------------
def parseThresholds(type : int, lst : List[typing.Tuple[AnyStr, AnyStr]]):
  th = []
  if lst is None: return th
  for target, thr in lst:
    if target in th:
      raise RuntimeError(f"multiple thresholds given for target '{target}'")
    th.append(TThreshold(type, thr, target, thresholdMapper))
  return th

#------------------------------------------------------------------------------
def thresholdMapper(thr : TThreshold, lbl : AnyStr) -> bool:
  if thr.Target == '+': return lbl == totalPerfLabel
  target=thr.Target.strip()
  m=re.match(r"([^\s]*)", target)
  return m.group(1).lower() == target.lower()

# Parsing the thresholds
warningThresholds  = parseThresholds(TNagios.WARNING, args.warn)
criticalThresholds = parseThresholds(TNagios.CRITICAL, args.crit)
Nagios.AddTheshold(warningThresholds)
Nagios.AddTheshold(criticalThresholds)

bacula = TBacula(f"{args.host}:{args.port}//{args.db}", args.dbuser, args.dbpass)

client = TClient(bacula, "wiki", args.job)
client.GetBackupStatus()
Nagios.ReturnResult()
