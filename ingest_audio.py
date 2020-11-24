"""
This class handles a single broadcast url

Input Args:
    1: url to the broadcast
    2: Name
    3: Output path directory

Steps:
    1: create output dir, if exists then clear it
    2: set a timer
    3: set a daemon thread and open a wget process, passing in the url, pipe 
"""