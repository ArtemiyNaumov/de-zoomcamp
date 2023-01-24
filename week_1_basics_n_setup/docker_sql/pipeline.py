import pandas as pd
import sys

def parse_arguments():
    counter = 1
    for arg in sys.argv:
        print(f'The {counter} argument is: {arg}')
        counter += 1

print('Pipeline started!')
print(f'Pandas version - {pd.__version__}')
parse_arguments()
print('Pipeline finished!')