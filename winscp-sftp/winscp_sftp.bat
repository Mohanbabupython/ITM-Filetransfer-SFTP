
rem 2418-Mohanbabu To transfer logfiles from windows server to SFTP server using batscript. 

@echo off
setlocal enabledelayedexpansion

rem Define AMC values in a separate file (amc.txt)
set "amc_file = amc.txt"
set "amc_values="
for /f "delims=" %%a in (%amc_file%) do (
    set "amc_values= !amc values!,%%a"
)
set "amc_values=!amc values:~1!" ::Remove leading comma

rem Loop through AMC values
for %%a in (!amc_values!) do (
    set "amc=%%a"
    
    for /f "tokens=1,2 delims=," %%i in (source_destination_paths.txt) do (
        set "source=%%i"
        echo source:!source!
        set "destination=%%j"
        echo destination: !destination!

        for /f "tokens=1-3 delims=/" %%a in ("%date%") do (
            set "log date=%%c-%%a-%%b"
            set "mm=%date:~4,2%"
            set "dd=%date:~7,2%"
            set "yy=%date:~10,4%"

            rem echo log date:!log date!
            rem echo exdate:!dd!-!mm!-!yy!

            rem Convert the month from to full month name
            for /f "tokens-1-2 delims=/" %%m in ('echo %%b') do (
                if "!mm!" == "01" Set "month=Jan"
                if "!mm!" == "02" set "month=Feb"
                if "!mm!" == "03" set "month=Mar"
                if "!mm!" == "04" set "month=Apr"
                if "!mm!" == "05" set "month=May"
                if "!mm!" == "06" set "month=Jun"
                if "!mm!" == "O7" set "month=Jul"
                if "!mm!" == "08" set "month=Aug"
                if "!mm!" == "09  set "month=Sep"
                if "!mm!" == "10" set "month=0ct"
                if "!mm!" == "11" set "month=Nov"
                if "!mm!" == "12" set "month=Dec"
             )

             rem Construct the desired date format
             set "formatted_date=!dd!-!month!-yy!"
             set "formatted date1 =!dd!!month!!yy!"
             set "formatted date2 =LOG_!yy!!mm!™
        )

             rem Read credentials from file
             for /f "tokens=1-3 delims=:" %%x in (credentials.txt) do (
                 set "username=%%x"
                 set "password_hostname=%%y"
                 set "filemask=%%z"
        )

         rem logfile paths & sftp server details..
         "C:\Program Files (x86)\WinSCP\WinSCP.com" ^
          /log="D:\data_transfer_sftp_log\WinSCP_!log date!.log" /ini=nul ^
          /command "option batch on" ^
             "echo Transferring !source! to !destination! on !log date! >> D:\data_transfer_sftp_log\WinSCP_!log date!.log" ^
             "open sftp://!username!:!password_hostname!/ -hostkey=""ssh-rsa 2048 ITE8Mcsakc8g/KCpAZ3Bw4X3WtgGoYWCoHTLPvR65cc""" ^
             "put -presevetime -filemask=!filemask! ""!source!"" ""!destination!""" ^
             "exit
  )

)
endlocal
exit
