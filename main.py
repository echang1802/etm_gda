
import argparse
from classes import process

if __main__ == '__main__':

    parser = argparse.ArgumentParser(description='Etermax - Game Data Analyst')
    parser.add_argument('--inputFilename', required=True, help='Directory and name of file with data to read')
    parser.add_argument('--outputDirectory', required=True, help='Directory where to store the data')
    parser.add_argument('--daysToSimulate', required=True, help='Number od days to simulate')
    args = parser.parse_args()

    proc = process(args.inputFilename, args.outputDirectory)

    #proc.generate_user_info()

    proc.simulate(args.daysToSimulate)
