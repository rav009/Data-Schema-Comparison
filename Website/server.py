import web
from urllib.parse import unquote
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))
from Database import SqliteDAO
import datetime

urls = (
        "/diff", "diff",
        "/comment", "comment"
       )
app = web.application(urls, globals())
render = web.template.render('templates/',globals={'unquote':unquote, 'str':str})


class hello:
    def GET(self):
        return 'Hello, world!'


class diff:
    def GET(self):
        args = web.input()
        s= ""
        if "s" not in args.keys():
            s = (datetime.date.today() + datetime.timedelta(-14)).strftime("%Y-%m-%d 00:00:00")
        else:
            s = str(args.s)[0:10].replace("/", "-") + " 00:00:00"
        n = ""
        if "notlike" not in args.keys():
            n = "%_RETIRE,%_BK,%TMP_%,%_BAK%,%_1100"
        else:
            n = args.notlike
        hide = True
        if "hide" in args.keys() and str(args.hide).upper() == "TRUE":
            hide = True
        ddls = SqliteDAO.getChangedDDL(s, n)
        if ddls and len(ddls)>1:
            ddls = sorted(ddls, key=lambda d: d[3], reverse=True)
        if hide:
            ddls = [d for d in ddls if str(d[7]).strip() == '']
        return render.diff(ddls, s[0:10], n, hide)


class comment:
    def POST(self):
        args = web.input()
        SqliteDAO.UpdateComment(int(args.id), args.comment)
        return "Comment Updated."


if __name__ == "__main__":
    app.run()