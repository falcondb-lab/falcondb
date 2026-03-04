; FalconDB Windows Installer — Inno Setup 6.x
;
; Build via: packaging\inno\build.ps1 -Version 1.2.0
; Or directly: ISCC.exe /DAppVersion=1.2.0 FalconDB.iss

#ifndef AppVersion
  #define AppVersion "0.0.0"
#endif

#ifndef StageDir
  #define StageDir "..\..\dist\windows\stage"
#endif

#define AppName     "FalconDB"
#define ServiceName "FalconDB"

[Setup]
AppId={{8A1E5B6A-7B8C-4D41-9B0F-4A2B0B4C7D21}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher=FalconDB Contributors
DefaultDirName={autopf}\FalconDB
DefaultGroupName=FalconDB
DisableProgramGroupPage=yes
OutputDir=..\..\dist\windows
OutputBaseFilename=FalconDB-Setup-{#AppVersion}-x64
Compression=lzma2
SolidCompression=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
PrivilegesRequired=admin
UninstallDisplayIcon={app}\bin\falcon.exe
WizardStyle=modern

[Languages]
Name: "en"; MessagesFile: "compiler:Default.isl"

[CustomMessages]
en.PgPortLabel=PostgreSQL listen port (default: 5433):
en.HttpPortLabel=HTTP admin listen port (default: 8080):
en.FirewallTaskDesc=Add Windows Firewall inbound rules for FalconDB ports
en.PurgeDataTaskDesc=Delete all data in C:\ProgramData\FalconDB on uninstall

[Tasks]
Name: "firewall"; Description: "{cm:FirewallTaskDesc}"; Flags: unchecked
Name: "purgedata"; Description: "{cm:PurgeDataTaskDesc}"; Flags: unchecked

[Files]
Source: "{#StageDir}\bin\falcon.exe";  DestDir: "{app}\bin";  Flags: ignoreversion
Source: "{#StageDir}\LICENSE";          DestDir: "{app}";      Flags: ignoreversion
Source: "{#StageDir}\NOTICE";           DestDir: "{app}";      Flags: ignoreversion
Source: "{#StageDir}\VERSION";          DestDir: "{app}";      Flags: ignoreversion

[Dirs]
Name: "{commonappdata}\FalconDB\conf"
Name: "{commonappdata}\FalconDB\data"
Name: "{commonappdata}\FalconDB\logs"
Name: "{commonappdata}\FalconDB\certs"

[UninstallRun]
Filename: "{sys}\sc.exe"; Parameters: "stop FalconDB"; Flags: runhidden; RunOnceId: "StopSvc"
Filename: "{sys}\sc.exe"; Parameters: "delete FalconDB"; Flags: runhidden; RunOnceId: "DelSvc"
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall delete rule name=""FalconDB PostgreSQL"""; Flags: runhidden; RunOnceId: "DelFwPG"
Filename: "{sys}\netsh.exe"; Parameters: "advfirewall firewall delete rule name=""FalconDB HTTP Admin"""; Flags: runhidden; RunOnceId: "DelFwHTTP"

; ── Pascal Script ─────────────────────────────────────────────────────────────
[Code]

var
  PortPage: TWizardPage;
  PgPortEdit:   TEdit;
  HttpPortEdit: TEdit;
  PgPortLabel:  TLabel;
  HttpPortLabel: TLabel;

const
  RegKey = 'SOFTWARE\FalconDB';

function IsValidPort(const S: string): Boolean;
var
  V: Integer;
begin
  V := StrToIntDef(S, -1);
  Result := (V >= 1) and (V <= 65535);
end;

procedure CreatePortPage;
begin
  PortPage := CreateCustomPage(wpSelectDir,
    'Network Ports',
    'Choose the ports FalconDB will listen on.');

  PgPortLabel := TLabel.Create(PortPage);
  PgPortLabel.Parent   := PortPage.Surface;
  PgPortLabel.Caption  := 'PostgreSQL listen port (default: 5433):';
  PgPortLabel.Left     := 0;
  PgPortLabel.Top      := 0;
  PgPortLabel.AutoSize := True;

  PgPortEdit := TEdit.Create(PortPage);
  PgPortEdit.Parent  := PortPage.Surface;
  PgPortEdit.Left    := 0;
  PgPortEdit.Top     := PgPortLabel.Top + PgPortLabel.Height + 4;
  PgPortEdit.Width   := 100;
  PgPortEdit.Text    := '5433';

  HttpPortLabel := TLabel.Create(PortPage);
  HttpPortLabel.Parent   := PortPage.Surface;
  HttpPortLabel.Caption  := 'HTTP admin listen port (default: 8080):';
  HttpPortLabel.Left     := 0;
  HttpPortLabel.Top      := PgPortEdit.Top + PgPortEdit.Height + 16;
  HttpPortLabel.AutoSize := True;

  HttpPortEdit := TEdit.Create(PortPage);
  HttpPortEdit.Parent  := PortPage.Surface;
  HttpPortEdit.Left    := 0;
  HttpPortEdit.Top     := HttpPortLabel.Top + HttpPortLabel.Height + 4;
  HttpPortEdit.Width   := 100;
  HttpPortEdit.Text    := '8080';
end;

procedure InitializeWizard;
begin
  CreatePortPage;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if CurPageID = PortPage.ID then begin
    if not IsValidPort(PgPortEdit.Text) then begin
      MsgBox('PostgreSQL port must be a number between 1 and 65535.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
    if not IsValidPort(HttpPortEdit.Text) then begin
      MsgBox('HTTP admin port must be a number between 1 and 65535.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
    if PgPortEdit.Text = HttpPortEdit.Text then begin
      MsgBox('PostgreSQL port and HTTP port must be different.', mbError, MB_OK);
      Result := False;
      Exit;
    end;
  end;
end;

procedure WriteConfig(const InstallDir, PgPort, HttpPort: string);
var
  DataDir, ConfigPath, Content: string;
begin
  DataDir    := 'C:/ProgramData/FalconDB/data';
  ConfigPath := 'C:\ProgramData\FalconDB\conf\falcon.toml';

  Content :=
    'config_version = 3'                                        + #13#10 +
    ''                                                          + #13#10 +
    '[server]'                                                  + #13#10 +
    'pg_listen_addr = "0.0.0.0:' + PgPort + '"'                + #13#10 +
    'admin_listen_addr = "0.0.0.0:' + HttpPort + '"'           + #13#10 +
    'node_id = 1'                                               + #13#10 +
    'max_connections = 1024'                                    + #13#10 +
    'statement_timeout_ms = 0'                                  + #13#10 +
    'idle_timeout_ms = 300000'                                  + #13#10 +
    'shutdown_drain_timeout_secs = 30'                          + #13#10 +
    ''                                                          + #13#10 +
    '[server.auth]'                                             + #13#10 +
    'method = "trust"'                                          + #13#10 +
    ''                                                          + #13#10 +
    '[server.tls]'                                              + #13#10 +
    ''                                                          + #13#10 +
    '[storage]'                                                 + #13#10 +
    'data_dir = "' + DataDir + '"' + #13#10 +
    'wal_enabled = true'                                        + #13#10 +
    'memory_limit_bytes = 0'                                    + #13#10 +
    ''                                                          + #13#10 +
    '[wal]'                                                     + #13#10 +
    'group_commit = true'                                       + #13#10 +
    'flush_interval_us = 1000'                                  + #13#10 +
    'sync_mode = "fdatasync"'                                   + #13#10 +
    'segment_size_bytes = 67108864'                             + #13#10 +
    ''                                                          + #13#10 +
    '[replication]'                                             + #13#10 +
    'role = "standalone"'                                       + #13#10 +
    'grpc_listen_addr = "0.0.0.0:50051"'                       + #13#10 +
    'primary_endpoint = "http://127.0.0.1:50051"'              + #13#10 +
    ''                                                          + #13#10 +
    '[gc]'                                                      + #13#10 +
    'enabled = true'                                            + #13#10 +
    'interval_ms = 1000'                                        + #13#10 +
    'batch_size = 0'                                            + #13#10 +
    'min_chain_length = 2'                                      + #13#10;

  SaveStringToFile(ConfigPath, Content, False);
end;

procedure AddFirewallRule(const RuleName, Port, Proto: string);
var
  ResultCode: Integer;
begin
  Exec(ExpandConstant('{sys}') + '\netsh.exe',
    'advfirewall firewall add rule name="' + RuleName + '" dir=in action=allow protocol=' + Proto + ' localport=' + Port,
    '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure RegisterService(const AppDir: string);
var
  FalconExe, ConfigFile, BatFile, BatContent: string;
  ResultCode: Integer;
begin
  FalconExe  := AppDir + '\bin\falcon.exe';
  ConfigFile := 'C:\ProgramData\FalconDB\conf\falcon.toml';
  BatFile    := ExpandConstant('{tmp}') + '\falcondb_svc.bat';

  BatContent :=
    '@echo off' + #13#10 +
    'sc.exe stop FalconDB >nul 2>&1' + #13#10 +
    'sc.exe delete FalconDB >nul 2>&1' + #13#10 +
    'timeout /t 1 /nobreak >nul' + #13#10 +
    'sc.exe create FalconDB binPath= "\"' + FalconExe + '\" service dispatch --config \"' + ConfigFile + '\""' +
      ' start= auto DisplayName= "FalconDB Database Server"' + #13#10 +
    'sc.exe description FalconDB "FalconDB - PostgreSQL-compatible in-memory database"' + #13#10 +
    'sc.exe failure FalconDB reset= 86400 actions= restart/5000/restart/10000/restart/30000' + #13#10 +
    'sc.exe start FalconDB' + #13#10;

  SaveStringToFile(BatFile, BatContent, False);
  Exec('cmd.exe', '/c "' + BatFile + '"', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  PgPort, HttpPort, AppDir: string;
begin
  if CurStep = ssPostInstall then begin
    PgPort   := PgPortEdit.Text;
    HttpPort := HttpPortEdit.Text;
    AppDir   := ExpandConstant('{app}');

    WriteConfig(AppDir, PgPort, HttpPort);
    RegisterService(AppDir);

    RegWriteStringValue(HKLM, RegKey, 'PgPort', PgPort);
    RegWriteStringValue(HKLM, RegKey, 'HttpPort', HttpPort);

    if WizardIsTaskSelected('firewall') then begin
      AddFirewallRule('FalconDB PostgreSQL', PgPort, 'TCP');
      AddFirewallRule('FalconDB HTTP Admin', HttpPort, 'TCP');
    end;

    if WizardIsTaskSelected('purgedata') then
      RegWriteStringValue(HKLM, RegKey, 'PurgeOnUninstall', '1')
    else
      RegWriteStringValue(HKLM, RegKey, 'PurgeOnUninstall', '0');
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  RC: Integer;
  PurgeVal: string;
begin
  if CurUninstallStep = usPostUninstall then begin
    if RegQueryStringValue(HKLM, RegKey, 'PurgeOnUninstall', PurgeVal) and (PurgeVal = '1') then
      Exec('cmd.exe', '/c rmdir /s /q "C:\ProgramData\FalconDB"',
        '', SW_HIDE, ewWaitUntilTerminated, RC);
    RegDeleteKeyIfEmpty(HKLM, RegKey);
  end;
end;
