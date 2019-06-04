@echo off

set script_path=%~dp0
set current_dir=%script_path:~0,-1%
set this_script_name=%~n0
set exercise_index=%this_script_name:~-1%
set pack_script=%current_dir%\pack.bat

call %pack_script% %exercise_index%