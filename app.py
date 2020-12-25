import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datedelta
import datetime as dt
import pandas as pd
from flask import Flask, jsonify


# Database Setup

engine = create_engine("sqlite:///Resources/hawaii.sqlite", echo=False)

Base = automap_base()
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask Setup
app = Flask(__name__)

# Flask Routes

@app.route("/")
def home():
    
    return (f"Available Routes:<br/>"
            f"/api/v1.0/precipitation</br>"
            f"/api/v1.0/stations</br>"
            f"/api/v1.0/tobs</br>"
            f"/api/v1.0/<start></br>"
            f"/api/v1.0/<start>/<end></br>")

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Return precipitation for the last year period 

    session = Session(engine)

    # calculate start day as the latest day from observation
    start_day_point = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    start_day_point =dt.datetime.strptime(start_day_point[0], "%Y-%m-%d")
    start_day_point = start_day_point.date()

    # calculate one yaer ago from start day
    one_year_ago = start_day_point - dt.timedelta(days=365)

    # get data from db for one year
    prcp_by_date = session.query(Measurement.date, Measurement.prcp)\
                     .filter(Measurement.date <= start_day_point)\
                     .filter(Measurement.date >= one_year_ago).order_by(Measurement.date).all()  
    session.close()
    
    one_year_prcp = {}
    for i in range(len(prcp_by_date)):
        if isinstance(prcp_by_date[i][1], float):
            one_year_prcp[prcp_by_date[i][0]] = prcp_by_date[i][1]

    return jsonify(one_year_prcp)
    
@app.route("/api/v1.0/stations")
def stations():
    # return the list of all available stations

    session = Session(engine)
    all_stations = session.query(Station.id, Station.station, Station.name).all()
    session.close()

    return jsonify(all_stations)
      
@app.route("/api/v1.0/tobs")
def tobs():
    # Return temperature observation of the most active station for last year of data
    session = Session(engine)

    # set up start day as the latest day from observation
    start_day_point = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    start_day_point =dt.datetime.strptime(start_day_point[0], "%Y-%m-%d")
    start_day_point = start_day_point.date()

    # get date for one yaer ago from start day
    one_year_ago = start_day_point - dt.timedelta(days=365)

    # find the most active station for given period of time
    station_activity = session.query(Measurement.station, func.count(Measurement.station))\
                                .filter(Measurement.date <=  start_day_point)\
                                .filter(Measurement.date >= one_year_ago).group_by(Measurement.station)\
                                .order_by(func.count(Measurement.station).desc()).all()

    # get all temperature observation for given period of time for the most active station
    temp_obs = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == station_activity[0][0])\
                        .filter(Measurement.date <=  start_day_point)\
                        .filter(Measurement.date >= one_year_ago).all()
    session.close()
    return jsonify(temp_obs)

@app.route("/api/v1.0/<start>")
def start_date(start):
    # Return list of the min, max and avg temperature for all dates greater than and equal to the start date
    session = Session(engine)

    tobs_start_day = session.query(func.min(Measurement.tobs)\
                            .func.max(Measurement.tobs)\
                            .func.avg(Measurement.tobs))\
                            .filter(Measurement.date >= start).all()

    session.close()
    return jsonify(tobs_start_day)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Return lst of the min, max and avg temperature between given period.
    start_day = ''
    end_day = ''

    if start > end:
        start_day = end
        end_day = start
    else:
        start_day = start
        end_day = end
        
    session = Session(engine)

    tobs_start_end = session.query(func.min(Measurement.tobs)\
                            .func.max(Measurement.tobs)\
                            .func.avg(Measurement.tobs))\
                            .filter(Measurement.date >= start_day)\
                            .filter(Measurement.date <= end_day)\
                            .all()

    session.close()
    return jsonify(tobs_start_end)


if __name__ == "__main__":
    app.run(debug=True)