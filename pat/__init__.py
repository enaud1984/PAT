import logging

"""
Inizialize package's logger, set propagate to True for message logging to root handlers
"""
logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.getLogger(__name__).propagate = True
