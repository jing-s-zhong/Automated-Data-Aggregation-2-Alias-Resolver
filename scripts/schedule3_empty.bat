rem ====================================================
rem Schedule-3: Empty the DATA_ALIASING_RESOLVER
rem ----------------------------------------------------
rem Example: schedule3_empty.bat BI_TEST _CONTROL_LOGIC
rem ====================================================
@echo off
if [%1]==[] goto missDb
if [%2]==[] goto missSchema

@echo Emptying the test data of the data aggregator in %1.%2
snowsql ^
--config ..\..\config\.snowsql\config ^
-f .\schedule3_empty.snow ^
-o exit_on_error=true ^
-o quiet=true ^
-o friendly=true ^
-D db_name=%1 ^
-D sc_name=%2
@echo The test data of the data aggregator in %1.%2 is emptied
goto done

:missDb
@echo First argument for DB name is missing!
goto example

:missSchema
@echo Second argument for SCHEMA name is missing!

:example
@echo Example: schedule3_empty.bat BI_TEST _CONTROL_LOGIC

:done
