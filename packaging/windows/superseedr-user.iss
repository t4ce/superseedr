; SPDX-FileCopyrightText: 2025 The superseedr Contributors
; SPDX-License-Identifier: GPL-3.0-or-later

#ifndef AppVersion
#define AppVersion "dev"
#endif

#ifndef AppOutputVersion
#define AppOutputVersion "dev"
#endif

#ifndef OutputDir
#define OutputDir "..\..\target\installer\windows"
#endif

#ifdef PrivateBuild
#define AppName "superseedr private"
#define AppId "superseedr-private-user"
#else
#define AppName "superseedr"
#define AppId "superseedr-user"
#endif

#define Publisher "The superseedr Contributors"
#define AppExeName "superseedr.exe"
#define AppIcon "..\..\assets\app_icon.ico"
#define AppBinary "..\..\target\release\superseedr.exe"

[Setup]
AppId={#AppId}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#Publisher}
AppPublisherURL=https://github.com/Jagalite/superseedr
AppSupportURL=https://github.com/Jagalite/superseedr/issues
AppUpdatesURL=https://github.com/Jagalite/superseedr/releases
DefaultDirName={localappdata}\Programs\superseedr
DefaultGroupName=superseedr
DisableProgramGroupPage=yes
OutputDir={#OutputDir}
#ifdef PrivateBuild
OutputBaseFilename=superseedr-private-{#AppOutputVersion}-x64-setup
#else
OutputBaseFilename=superseedr-{#AppOutputVersion}-x64-setup
#endif
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
SetupIconFile={#AppIcon}
UninstallDisplayIcon={app}\app_icon.ico
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ChangesAssociations=yes

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Shortcuts:"; Flags: unchecked
Name: "path"; Description: "Add superseedr to PATH for this user"; GroupDescription: "Windows integration:"; Flags: checkedonce
Name: "associations"; Description: "Register magnet links and .torrent files for this user"; GroupDescription: "Windows integration:"; Flags: checkedonce

[Files]
Source: "{#AppBinary}"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#AppIcon}"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\superseedr"; Filename: "{app}\{#AppExeName}"; WorkingDir: "{app}"; IconFilename: "{app}\app_icon.ico"
Name: "{autodesktop}\superseedr"; Filename: "{app}\{#AppExeName}"; WorkingDir: "{app}"; IconFilename: "{app}\app_icon.ico"; Tasks: desktopicon

[Registry]
Root: HKCU; Subkey: "Software\Classes\magnet"; ValueType: string; ValueName: ""; ValueData: "URL:Magnet Protocol"; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\magnet"; ValueType: string; ValueName: "URL Protocol"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\magnet\DefaultIcon"; ValueType: string; ValueName: ""; ValueData: """{app}\app_icon.ico"",0"; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\magnet\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}"" ""%1"""; Flags: uninsdeletekey; Tasks: associations

Root: HKCU; Subkey: "Software\Classes\.torrent"; ValueType: string; ValueName: ""; ValueData: "superseedr.torrent"; Flags: uninsdeletevalue; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\.torrent"; ValueType: string; ValueName: "Content Type"; ValueData: "application/x-bittorrent"; Flags: uninsdeletevalue; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\superseedr.torrent"; ValueType: string; ValueName: ""; ValueData: "Torrent File (superseedr)"; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\superseedr.torrent\DefaultIcon"; ValueType: string; ValueName: ""; ValueData: """{app}\app_icon.ico"",0"; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\superseedr.torrent\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}"" ""%1"""; Flags: uninsdeletekey; Tasks: associations

Root: HKCU; Subkey: "Software\Classes\Applications\{#AppExeName}"; ValueType: string; ValueName: "FriendlyAppName"; ValueData: "superseedr"; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\Applications\{#AppExeName}\SupportedTypes"; ValueType: string; ValueName: ".torrent"; ValueData: ""; Flags: uninsdeletekey; Tasks: associations
Root: HKCU; Subkey: "Software\Classes\Applications\{#AppExeName}\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}"" ""%1"""; Flags: uninsdeletekey; Tasks: associations

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch superseedr"; Flags: nowait postinstall skipifsilent unchecked

[Code]
const
  SuperseedrHwndBroadcast = $ffff;
  SuperseedrWmSettingChange = $001A;
  SuperseedrSmtoAbortIfHung = $0002;
  EnvSubKey = 'Environment';
  InstallerSubKey = 'Software\superseedr\Installer';
  PathMarkerName = 'UserPath';

function SendMessageTimeout(hWnd: Longint; Msg: Longint; wParam: Longint; lParam: String; fuFlags: Longint; uTimeout: Longint; var lpdwResult: Longint): Longint;
  external 'SendMessageTimeoutW@user32.dll stdcall';

function UserPathEntry(): String;
begin
  Result := ExpandConstant('{app}');
end;

function NormalizePathEntry(Value: String): String;
begin
  Result := Lowercase(Value);
  StringChangeEx(Result, '/', '\', True);
  while (Length(Result) > 0) and (Result[Length(Result)] = '\') do
  begin
    Delete(Result, Length(Result), 1);
  end;
end;

function PathContainsEntry(PathValue: String; Entry: String): Boolean;
var
  Remaining: String;
  Segment: String;
  Separator: Integer;
  Needle: String;
begin
  Result := False;
  Needle := NormalizePathEntry(Entry);
  Remaining := PathValue;

  while Remaining <> '' do
  begin
    Separator := Pos(';', Remaining);
    if Separator > 0 then
    begin
      Segment := Copy(Remaining, 1, Separator - 1);
      Delete(Remaining, 1, Separator);
    end
    else
    begin
      Segment := Remaining;
      Remaining := '';
    end;

    if NormalizePathEntry(Segment) = Needle then
    begin
      Result := True;
      Exit;
    end;
  end;
end;

procedure BroadcastEnvironmentChange();
var
  ResultCode: Longint;
begin
  SendMessageTimeout(SuperseedrHwndBroadcast, SuperseedrWmSettingChange, 0, 'Environment', SuperseedrSmtoAbortIfHung, 5000, ResultCode);
end;

procedure AddToUserPath();
var
  PathValue: String;
  Entry: String;
  NewPath: String;
begin
  Entry := UserPathEntry();
  if not RegQueryStringValue(HKCU, EnvSubKey, 'Path', PathValue) then
  begin
    PathValue := '';
  end;

  if PathContainsEntry(PathValue, Entry) then
  begin
    Exit;
  end;

  if PathValue = '' then
  begin
    NewPath := Entry;
  end
  else if PathValue[Length(PathValue)] = ';' then
  begin
    NewPath := PathValue + Entry;
  end
  else
  begin
    NewPath := PathValue + ';' + Entry;
  end;

  RegWriteExpandStringValue(HKCU, EnvSubKey, 'Path', NewPath);
  RegWriteDWordValue(HKCU, InstallerSubKey, PathMarkerName, 1);
  BroadcastEnvironmentChange();
end;

procedure RemoveFromUserPath();
var
  PathValue: String;
  Entry: String;
  Remaining: String;
  Segment: String;
  NewPath: String;
  Separator: Integer;
begin
  if not RegQueryStringValue(HKCU, EnvSubKey, 'Path', PathValue) then
  begin
    Exit;
  end;

  Entry := UserPathEntry();
  Remaining := PathValue;
  NewPath := '';

  while Remaining <> '' do
  begin
    Separator := Pos(';', Remaining);
    if Separator > 0 then
    begin
      Segment := Copy(Remaining, 1, Separator - 1);
      Delete(Remaining, 1, Separator);
    end
    else
    begin
      Segment := Remaining;
      Remaining := '';
    end;

    if (Segment <> '') and (NormalizePathEntry(Segment) <> NormalizePathEntry(Entry)) then
    begin
      if NewPath = '' then
      begin
        NewPath := Segment;
      end
      else
      begin
        NewPath := NewPath + ';' + Segment;
      end;
    end;
  end;

  if NewPath <> PathValue then
  begin
    RegWriteExpandStringValue(HKCU, EnvSubKey, 'Path', NewPath);
    BroadcastEnvironmentChange();
  end;

  RegDeleteValue(HKCU, InstallerSubKey, PathMarkerName);
  RegDeleteKeyIfEmpty(HKCU, InstallerSubKey);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if (CurStep = ssPostInstall) and IsTaskSelected('path') then
  begin
    AddToUserPath();
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if (CurUninstallStep = usPostUninstall) and RegValueExists(HKCU, InstallerSubKey, PathMarkerName) then
  begin
    RemoveFromUserPath();
  end;
end;
