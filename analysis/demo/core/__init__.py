import logging
logging.basicConfig(filename='/vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log', filemode='a', level=logging.INFO,
                    format='[%(levelname)s][%(asctime)s][%(name)s]%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("lhd_logger")


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))