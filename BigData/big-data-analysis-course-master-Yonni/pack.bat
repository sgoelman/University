echo off
setlocal EnableDelayedExpansion

set exe_index=%1

echo Making sure that exercise index is not missing
if "%exe_index%"=="" (
	echo Missing parameter: exercise index
	pause
	exit 1
) else (
	echo exercise index is: %exe_index%
)

set script_path=%~dp0
set current_dir=%script_path:~0,-1%
set target_dir_name=bin
set target_dir=%current_dir%\%target_dir_name%
set exercise_dir=%current_dir%\exercises\exe%exe_index%
set pack_script=pack.bat
set zip_name=exe%exe_index%
set zip_file=%zip_name%.zip
set zip_file_path=%current_dir%\%zip_name%.zip
set exclude_extensions=bat gitignore zip tmp
set extension_to_rename=bash
set extension_new_name=txt
set exercise_main_file=%exercise_dir%\exe%exe_index%.ipynb
set exercise_versioned_file=%exercise_dir%\exe%exe_index%_v1.ipynb
set exercise_main_file_description=jupyter notebook file

echo Making sure that exercise %exe_index% directory exists.
if not exist %exercise_dir% (
	echo cannot pack exercise %exe_index%, because the directory %exercise_dir% does not exists.
	pause
	exit 1
)

echo Making sure that exercise %exe_index% directory is not empty.
if not exist %exercise_dir%\* (
	echo cannot pack exercise %exe_index%, because the directory %exercise_dir% is empty.
	pause
	exit 1
)

echo Making sure that %exercise_main_file_description% is not missing.
if not exist %exercise_main_file% (
	if not exist %exercise_versioned_file% (
		echo cannot pack exercise %exe_index%, because the %exercise_main_file_description%: %exercise_main_file% is missing.
		pause
		exit 1
	)
)

echo Create %target_dir% directory, if not exists.
if not exist %target_dir% mkdir %target_dir%
if %ERRORLEVEL% == 1 (
    echo failed to create the directory: %target_dir%
	pause
	exit 1
)

echo Copy files from %current_dir% directory to %target_dir% directory.
xcopy /y %current_dir% %target_dir%
if %ERRORLEVEL% == 1 (
    echo failed to copy files to the directory: %target_dir%
	pause
	exit 1
) else (
	echo files have been successfully copied from %current_dir% to %target_dir%
)

echo Remove irrelevant files
for %%a in (%exclude_extensions%) do (
	if exist %target_dir%\*.%%a (
		del /F %target_dir%\*.%%a
	)
)

echo Change %extension_to_rename% files extensions to .%extension_new_name% if relevant
FOR %%A IN (%target_dir%\*.%extension_to_rename%) DO (
	REN "%%~fA" "%%~nA.%extension_to_rename%.%extension_new_name%"
	if %ERRORLEVEL% == 1 (
		echo failed to rename .%extension_to_rename% files to  %zip_file_path% to .%extension_new_name%
		pause
	exit 1
	) else (
		echo .%extension_to_rename% files has been successfully renamed to .%extension_new_name%
	)
)

echo Remove .%extension_to_rename% files if exists
if exist %target_dir%\*.%extension_to_rename% (
	del /F %target_dir%\*.%extension_to_rename%
)

echo copy exercise %exe_index% files, from %exercise_dir% to %target_dir%
xcopy /y %exercise_dir% %target_dir%
if %ERRORLEVEL% == 1 (
    echo failed to copy exercise files to the directory: %target_dir%
	pause
	exit 1
) else (
	echo exercise  have been successfully copied from %exercise_dir% to %target_dir%
)

echo zip files in %target_dir% directory.
for /d %%X in (%target_dir%\) do "c:\Program Files\7-Zip\7z.exe" a "%zip_file%" "%%X\"

echo move %zip_file_path% to %target_dir% directory.
move %zip_file_path% %target_dir%
if %ERRORLEVEL% == 1 (
    echo failed to move %zip_file_path% to the directory: %target_dir%
	pause
	exit 1
) else (
	echo %zip_file_path% has been successfully moved to %target_dir%
)

