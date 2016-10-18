"""
Runs the Amoeba simulations.
"""
import os

adapt_cmd = "fab setup:tpch_one table_name:%s simulator_adapt:%d"
no_adapt_cmd = "fab setup:tpch_one table_name:%s simulator_noadapt"

adapts = ['tpch', 'opt_tpch']
cs = [1,2,4,8]

for table in adapts:
  for c in cs:
    print "Running %s with c %d" % (table, c)
    os.system(adapt_cmd % (table, c))
    os.system("scp mdindex@istc2:~/logs/sim_adapt.log result_logs/adapt_%s_%d.log" % (table, c))

no_adapts = ['tpch_range_tree', 'tpch_hybrid_range_tree', 'tpch_kdtree', 'tpch']
for table in no_adapts:
  print "Running %s no adapt" % table
  os.system(no_adapt_cmd % table)
  os.system("scp mdindex@istc2:~/logs/sim_noadapt.log result_logs/noadapt_%s.log" % table)

