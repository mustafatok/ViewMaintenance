"C:\Users\Jan Adler\Desktop\plink.exe" -t -batch -pw MTVdvrDV$15 jan@deltaserver1 rm /usr/local/hbaselib/regionServerVM.jar
"C:\Program Files (x86)\WinSCP\winscp.exe" /console /command "option batch abort" "open jan@deltaserver1" "put E:\Masterpraktikum\ViewMaintenanceInWDP\build\regionServerVM.jar /usr/local/hbaselib/"  "exit"

