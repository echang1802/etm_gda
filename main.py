
import argparse
from classes import process

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Etermax - Game Data Analyst')
    parser.add_argument('--inputFilename', required = True, help = 'Directory and name of file with data to read')
    parser.add_argument('--outputDirectory', required = True, help = 'Directory where to store the data')
    parser.add_argument('--daysToSimulate', default = 10, help = 'Number of days to simulate')
    parser.add_argument('--agents', default = 4, help = 'Number of simultaneous processing')
    args = parser.parse_args()

    proc = process(args.inputFilename, args.outputDirectory)

    proc.generate_user_info()

    proc.simulate(args.daysToSimulate)
