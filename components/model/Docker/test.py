import time
with open('test.txt', 'a') as fp:
    pass
    fp.write(str(time.time()))
    fp.write("\n")

