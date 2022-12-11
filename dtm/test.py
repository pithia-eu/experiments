import pandas
import numpy
import seaborn as sns
import matplotlib.pylab as plt
fig = plt.figure(figsize=[8,8])
# df = pandas.read_csv(f'DTM20F107Kp_{data}.datx', sep='     ', header=None)
latidute = []
counter = 0
for i in range(-87,87):
    if counter == 3:
        latidute.append(i)
        counter = 0
    elif counter == 0:
        latidute.append(i)
    counter += 1
latidute.append(87)
df = pandas.read_csv(f'test.datx', sep='     ', header=None, engine='python')
lat_array = numpy.array(latidute)
df = df.set_index(keys=lat_array)
print(lat_array)

print(df)
ax = sns.heatmap(df, linewidth=0, cmap='Spectral_r')
plt.ylabel('latitude')
plt.xlabel('solar local time')
plt.title(f'DTM20F107Kp_.data')
plt.show()
exit()
#
# df = df.replace('E', 'e', regex=True)
# print(df)
# #
# # # convert notation to the one pandas allows
# df = df.apply(pandas.to_numeric)
# print(df)
# exit()
#

v = df.values

# Program to plot 2-D Heat map
# using matplotlib.pyplot.pcolormesh() method
ax = sns.heatmap( df, linewidth = 0 , cmap = 'Spectral_r' )

print(v)


plt.show()
exit()
plt.pcolor(df.values)
plt.show()
# fig.savefig('test1.png')