g++ -o libpysnmp.so -std=c++0x -fPIC -shared -I../include ../src/snmp_agent.cpp ../src/snmp_counter_table.cpp ../src/snmp_row.cpp ../src/log.cpp ../src/logger.cpp ../src/snmp_c_linkage.cpp `net-snmp-config --netsnmp-agent-libs`

LIBRARY_PATH=. CC="gcc -I../include" python pysnmp.py

LD_LIBRARY_PATH=. python test_python_snmp.py
