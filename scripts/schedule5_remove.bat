rem ====================================================
rem Schedule-5: Remove the DATA_ALIASING_RESOLVER
rem ----------------------------------------------------
rem Example: schedule5_remove.bat BI_TEST _CONTROL_LOGIC
rem ====================================================
@echo off
if [%1]==[] goto missDb
if [%2]==[] goto missSchema

@echo Removing the data aggregator from %1.%2
snowsql ^
--config ..\..\config\.snowsql\config ^
-f .\schedule5_remove.snow ^
-o exit_on_error=true ^
-o quiet=true ^
-o friendly=true ^
-D db_name=%1 ^
-D sc_name=%2
@echo The data aggregator is removed from %1.%2
goto done

:missDb
@echo First argument for DB name is missing!
goto example

:missSchema
@echo Second argument for SCHEMA name is missing!

:example
@echo Example: schedule5_remove.bat BI_TEST _CONTROL_LOGIC

:done
