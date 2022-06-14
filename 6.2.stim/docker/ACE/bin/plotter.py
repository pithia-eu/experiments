import os,sys
sys.path.insert(0, os.path.abspath('../'))
import typer
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
import ciso8601
import numpy as np
from app import CONF as cfg
from Lib.database.base import DBUtils

from app import ACELogger

app = typer.Typer()

def plot(start,end,data):
    cpath = "magdata_{:%Y%m%dT%H%M%S}_{:%Y%m%dT%H%M%S}.png".format(start,end)
    cpath = os.path.join(cfg['PLOT_PATH'], cpath)

    plotmap = {
        'Bmag': {'color': '#eb7434', 'marker': 'o', 'markercolor': '#eb7434dd', 'markersize': 3},
        'Bx': {'color': '#d6ae0f', 'marker': 's', 'markercolor': '#d6ae0fdd', 'markersize': 2.2},
        'By': {'color': '#3dfcc9', 'marker': '^', 'markercolor': '#3dfcc9dd', 'markersize': 2.8},
        'Bz': {'color': '#3d9dfc', 'marker': 'D', 'markercolor': '#3d9dfcdd', 'markersize': 2.5},
    }

    fig, ax2 = plt.subplots(1, 1, figsize=(12, 6), dpi=120, facecolor='w', edgecolor='k')

    ax2.grid(True, linestyle=':')

    x = [d['ACE_date'] for d in data]
    storm = np.array([0,] + [d['flg_scstrm'] for d in data] + [0,],np.int8)
    diff = storm[1:]-storm[0:-1]
    sidx = np.where(diff==1)
    eidx = np.where(diff == -1)
    pdata = {
        'Bmag' : [d['Bmag'] for d in data],
        'Bx' : [d['Bx'] for d in data],
        'By' : [d['By'] for d in data],
        'Bz' : [d['Bz'] for d in data]
    }

    legend2 = {'lines': [], 'labels': []}
    max_relstdne = 0
    for key in pdata.keys():
        tdata = pdata[key]
        color = plotmap[key]['color']
        marker = plotmap[key]['marker']
        markercolor = plotmap[key]['markercolor']
        markersize = plotmap[key]['markersize']

        y2 = np.array(pdata[key],dtype=np.float32)
        mask2 = np.isfinite(y2)

        allnan = np.all(np.isnan(y2))

        line2, = ax2.plot(x, y2, lw=1, color=color,
                          marker=marker, markersize=markersize, markerfacecolor=markercolor,
                          markeredgewidth=.5, markeredgecolor='#222222AA',
                          )

        legend2['lines'].append(line2)
        legend2['labels'].append(f'{key}')

    for s,e in zip(sidx[0],eidx[0]):
        ax2.axvspan(x[s-1], x[e-1], alpha=0.2, color='red')

    ax2.legend(legend2['lines'], legend2['labels'])

    ax2.tick_params(labelsize=9)

    ax2.set_title('MAG Data between {:%Y-%m-%dT%H:%M:%S} / {:%Y-%m-%dT%H:%M:%S}'.format(start, end))

    #ax2.set_ylabel('Val')
    ax2.set_xlabel('Datetime')

    #if max_relstdne <= 150:
    #    ax2.set_ylim(0, 150.1)

    plt.subplots_adjust(left=0.08, right=0.97, top=0.93, bottom=0.09)
    if os.path.exists(cpath):
        os.unlink(cpath)
    plt.savefig(cpath)
    plt.close()


_Q = """
select a.id,a.ACE_date,a.Bmag,a.Bx,a.By,a.Bz,
CASE WHEN b.Storm_Onset THEN true ELSE FALSE END as flg_scstrm,
b.Storm_Onset,b.Storm_Offset,b.Bz_minTime,c.Bz as Bz_min
from magdata a left join stormsace b 
ON a.ACE_date>=b.Storm_Onset and (a.ACE_date<coalesce(b.Storm_Offset,@MaxCollatingDate))
left join magdata c ON b.Bz_minTime=c.ACE_date
where a.ACE_date>= %s and a.ACE_date< %s
order by a.ACE_date asc;
"""

@app.command()
def main(
        start: datetime = typer.Argument(..., help='Start query date'),
        end: datetime = typer.Argument(..., help='End query date'),
    ):
    """
    ACE Plotter:
    Plots Hourly RTSW|ACE MAG Ionospheric parameters (Bmag,Bx,By,Bz)
    in
    data for acquisitions requested by time interval.
    """

    if (end-start)>timedelta(days=1000):
        ACELogger.logger.critical(f"Interval [{start.isoformat()} {end.isoformat()}] > 1000days")
        exit(0)

    ACELogger.logger.info(f"Plotting data for [{start.isoformat()} {end.isoformat()}]")
    data = None
    with DBUtils(rawAsDict=True) as dbo:
        data = dbo.rawdb.fetch(_Q, (start,end))

    if not data:
        ACELogger.logger.info(f"No data found for interval [{start.isoformat()} {end.isoformat()}]")

    plot(start,end,data)



if __name__ == '__main__':
    sys.exit(app())