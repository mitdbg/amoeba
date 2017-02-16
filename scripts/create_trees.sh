#!/bin/bash

fab setup:tpch_one create_robust_tree
fab setup:tpch_one table_name:tpch_kdtree create_kdtree
fab setup:tpch_one table_name:tpch_range_tree create_range_tree
fab setup:tpch_one table_name:tpch_hybrid_range_tree create_hybrid_range_tree

