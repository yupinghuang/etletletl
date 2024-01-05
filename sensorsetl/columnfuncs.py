from pyspark.sql import WindowSpec, Column
from pyspark.sql import functions as F


def dydx(y: Column, x: Column, win: WindowSpec) -> Column:
    dx = (F.lead(x).over(win) - F.lag(x).over(win))
    dy = F.lead(y).over(win) - F.lag(y).over(win)
    deriv = dy / dx
    return deriv


def d2ydx2(y: Column, x: Column, win: WindowSpec) -> Column:
    dx = F.lead(x).over(win) - F.lag(x).over(win)
    yplus = F.lead(y).over(win)
    yminus = F.lag(y).over(win)
    deriv = (yplus - 2 * y + yminus) / F.pow(dx, 2)
    return deriv

def norm(v1: Column, v2: Column, v3: Column) -> Column:
    return F.sqrt(v1 ** 2 + v2 ** 2 + v3 ** 2)