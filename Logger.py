import logging
import os


if not os.path.exists('logs'):
    os.mkdir("logs")
    
logging.root.handlers = []

#format for logging
FORMAT = "%(asctime)s : %(levelname)s : %(funcName)s : %(message)s"

logging.basicConfig(format = FORMAT, level=logging.INFO , filename='logs/logFile.log')

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)

# set a format which is simpler for console use
formatter = logging.Formatter(FORMAT)
console.setFormatter(formatter)
logger  = logging.getLogger("")

