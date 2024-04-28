from dotenv import dotenv_values
import pandas as pd

# 3) Files in Transform folder now become existing files
# 4) Files in raw folder now becomes incoming files
# 5) Use the check last updated to check if the dates of the raw file is diferent from that of the transform folder
# 6) If dates are different, append
# 7) If dates are same, ignore and add a print message 'Dates already exist'

# def check_last_updated():
#     # if the datetime of maximum current data is different from the incoming response data, then append it
# 
# 
#

#with open('csv A', 'r') as open1:

csv1 = pd.read_csv(r"C:\Users\Admin\Desktop\Project 1 - web scrapper (jazzy investment)\csv A.csv")
print(csv1)

csv2 = pd.read_csv(r"C:\Users\Admin\Desktop\Project 1 - web scrapper (jazzy investment)\csv B.csv")
print(csv2)

the2 = csv2.append(csv1)
print