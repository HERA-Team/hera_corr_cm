#!/usr/bin/env python

import time
import redis
import datetime
import json


def start_row():
  return '<div class="row">'


def end_row():
  return '</div>'


def start_column():
  return '<div class="column">'


def end_column():
  return '</div>'


r = redis.Redis('localhost')

html_header = """
<head>
<meta http-equiv=\"refresh\" content=\"60\">\n
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
* {
    box-sizing: border-box;
}

/* Create three equal columns that floats next to each other */
.column {
    float: left;
    width: 33.33%;
    padding: 10px;
}

/* Clear floats after the columns */
.row:after {
    content: "";
    display: table;
    clear: both;
}

/* Responsive layout - makes the three columns stack on top of each other instead of next to each other */
@media screen and (max-width: 800px) {
    .column {
        width: 100%;
    }
}

/* Font definitions */
h0 {
    font-family: Arial;
    font-size: 2em;
    font-weight: bold;
}

h1 {
    font-family: Arial;
    font-size: 1.5em;
    font-weight: normal;
    font-style: italic;
}


h2 {
    font-family: "Times New Roman", Times, serif;
    font-size: 1.5em;
    font-style: italic;
    font-weight: bold;
}

h3 {
    font-family: "Times New Roman", Times, serif;
    font-size: 1em;
    font-style: normal;
    font-weight: normal;
}

</style>
</head>
"""

current_col = 0
cols = {"ant":1, "node":2, "snap":3}

with open("/var/www/html/nodes.html", "w") as fh:
  fh.write("<html>\n")
  fh.write(html_header)
  fh.write("  <body>\n")
  fh.write("    <h0>{time:s} UTC</h0>\n".format(time.ctime()))
  # First print scripts status
  for k in sorted(r.keys()):
    if k.startswith("status:script:"):
      fh.write("<h1>{name:s}: "
               "{key:s}</h1>".format(name=k.lstrip("status:script:"),
                                     key=r[k])
               )
  fh.write(start_row())
  for k in sorted(r.keys()):
    if k.startswith("status:"):
      stattype = k.split(":")[1]
      if stattype not in list(cols.keys()):
          continue
      if current_col != cols[stattype]:
          if current_col != 0:
              fh.write(end_column())
          fh.write(start_column())
          current_col = cols[stattype]
      fh.write("    <h2>{key:s}</h2>\n".format(key=k))
      x = r.hgetall(k)
      for key, val in sorted(x.items()):
        if key.startswith('histogram'):
            continue
        if key.startswith("eq"):
          if val != "None":
              val = "%s..." % json.loads(val)[0:3]
        if key.startswith("power"):
          if val == "0":
            style = "color:red;"
          else:
            style = "color:green;"
        elif key == 'timestamp':
            try:
                then = datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
            except:
                then = datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S.%f")
            now  = datetime.datetime.now()
            diff = now - then
            val += " UTC" # Print the timezone for clarity
            if diff.total_seconds() > 60:
              style = "color:red;"
            else:
              style = "color:green;"
        else:
          style = "color:black;"
        fh.write("      <h3 style=\"{style:s}\"><pre "
                 "class='tab'>{key:s}: {val:s}</pre></h3>\n"
                 .format(style=style, key=key, val=val.replace('\n', '<br>'))
                 )
      fh.write("\n")
      fh.write("<hr>\n")

  fh.write(end_column())
  fh.write(end_row())
  fh.write("  </body>\n")
  fh.write("</html>\n")
