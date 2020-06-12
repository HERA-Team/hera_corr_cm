#!/usr/bin/env python

import time
import redis
import numpy as np
from astropy.time import Time

# Two redis instances run on this server.
# port 6379 is the hera-digi mirror
# port 6380 is the paper1 mirror
r = redis.Redis('localhost', 6379, decode_responses=True)

n_ants = 192
# Generate frequency axis
NCHANS = int(2048 // 4 * 3)
NCHANS_F = 8192
NCHAN_SUM = 4
frange = np.linspace(0, 250e6, NCHANS_F + 1)[1536:1536 + (8192 // 4 * 3)]
# average over channels
frange = frange.reshape(NCHANS, NCHAN_SUM).sum(axis=1) / NCHAN_SUM
frange_str = ', '.join('{freq:f}'.format(freq=freq)for freq in frange)
linenames = []

# All this code does is build an html file
# containing a bunch of javascript nonsense.
# define the start and end text of the file here,
# then dynamically populate the data sections.

html_preamble = '''
<!DOCTYPE html>
   <head>
     <meta http-equiv=\"refresh\" content=\"300\">\n
     <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
   </head>
   <body>
'''

plotly_preamble = '''
   <!-- Plotly chart will be drawn inside this div -->
   <div id="plotly-div"></div>
   <script>

'''

plotly_postamble = '''
    layout = {
      xaxis: {title: 'Frequency [MHz]'},
      yaxis: {title: 'Power [dB]'},
      height: 800,
      title: 'Autocorrelation Powers',
      margin: {l: 40, b: 0, r: 40, t: 30},
      hovermode: 'closest'
    };
    Plotly.plot('plotly-div', {data:data, layout:layout});

    </script>
'''

html_postamble = '''
  </body>
</html>
'''

got_time = False
n_signals = 0
with open('/var/www/html/powers.html', 'w') as fh:
  fh.write(html_preamble)
  fh.write(plotly_preamble)
  # Get time of plot
  t_plot_jd = np.fromstring(r['auto:timestamp'], dtype=np.float64)[0]
  t_plot_unix = Time(t_plot_jd, format='jd').unix
  print(t_plot_jd, t_plot_unix)
  got_time = True
  # grab data from redis and format it according to plotly's javascript api
  for i in range(n_ants):
    for pol in ['e', 'n']:
        # get the timestamp from redis for the first ant-pol
        if not got_time:
            t_plot_jd = float(r.hget('visdata://{i:d}/{j:d}/{i_pol:s}{j_pol:s}'
                                     .format(i=i, j=i, i_pol=pol, j_pol=pol),
                                     'time'
                                     )
                              )

            if t_plot_jd is not None:
                t_plot_unix = Time(t_plot_jd, format='jd').unix
                got_time = True
        linename = 'ant{ant:d}{pol:s}'.format(ant=i, pol=pol)
        d = r.get('auto:{ant:d}{pol:s}'.format(ant=i, pol=pol))
        #r.hget('visdata://%d/%d/%s%s' % (i,i,pol,pol), 'data')
        if d is not None:
            n_signals += 1
            linenames += [linename]
            fh.write('{name:s} = {\n'.format(name=linename))
            fh.write('  x: [{frange:s}],\n'.format(frange=frange_str))
            f = np.fromstring(d, dtype=np.float32)[0:NCHANS]
            f[f<10**-2.5] = 10**-2.5
            f = 10*np.log10(f)
            f_str = ', '.join('{freq:f}'.format(freq=freq) for freq in f)
            fh.write('  y: [{f_str:s}],\n'.format(f_str=f_str))
            fh.write("  name: '{name:s}',\n".format(name=linename))
            fh.write("  type: 'scatter'\n")
            fh.write('};\n')
            fh.write('\n')
  fh.write('data = [{name:s}];\n'.format(name=', '.join(linenames)))

  fh.write(plotly_postamble)
  fh.write('<p>Plots from {unix:s} UTC (JD: {jd:f})</p>\n'.format(unix=time.ctime(t_plot_unix), jd=t_plot_jd))
  fh.write('<p>Queried on {now:s} UTC</p>\n'.format(now=time.ctime()))
  #fh.write('<p>CMINFO source: %s</p>\n' % r['cminfo_source'])
  fh.write(html_postamble)

print('Got {n_sig:d} signals'.format(n_sig=n_signals))
