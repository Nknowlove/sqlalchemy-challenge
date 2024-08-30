# Import the dependencies.
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
from datetime import datetime, timedelta
from flask import Flask, jsonify
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# Reflect an existing database into a new model
Base = automap_base()
Base.prepare(engine, reflect=True)

# Print out class names to debug
print("Available classes:", Base.classes.keys())

# Save references to each table
Station = Base.classes.station  
Measurement = Base.classes.measurement

# Create a sessionmaker instance
Session = sessionmaker(bind=engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available API routes."""
    return (
        f"Welcome to the Assignment!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt; (Enter start date in YYYY-MM-DD format)<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt; (Enter start and end dates in YYYY-MM-DD format)<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session()
    try:
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        end_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=365)
        results = session.query(Measurement.date, Measurement.prcp).filter(
            Measurement.date >= start_date.strftime('%Y-%m-%d')
        ).order_by(Measurement.date).all()
        precipitation_dict = {date: prcp for date, prcp in results}
        return jsonify(precipitation_dict)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route("/api/v1.0/stations")
def stations():
    session = Session()
    try:
        # Query all stations from the station table
        results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
        
        # Create a list of dictionaries to hold the station data
        stations_list = []
        for station, name, latitude, longitude, elevation in results:
            station_dict = {
                "station": station,
                "name": name,
                "latitude": latitude,
                "longitude": longitude,
                "elevation": elevation
            }
            stations_list.append(station_dict)
        
        # Return the JSON list of stations
        return jsonify(stations_list)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    finally:
        session.close()

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session()
    try:
        # Identify the most active station (i.e., the station with the most temperature observations)
        most_active_station = (
            session.query(Measurement.station)
            .group_by(Measurement.station)
            .order_by(func.count(Measurement.station).desc())
            .first()
            .station
        )

        # Determine the last date in the dataset and calculate the start date for the previous year
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        end_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=365)

        # Query the temperature observations of the most active station for the previous year
        results = (
            session.query(Measurement.date, Measurement.tobs)
            .filter(Measurement.station == most_active_station)
            .filter(Measurement.date >= start_date.strftime('%Y-%m-%d'))
            .order_by(Measurement.date)
            .all()
        )

        # Format the results into a list of dictionaries
        tobs_list = [{'date': date, 'temperature': tobs} for date, tobs in results]

        # Return the JSON list of temperature observations
        return jsonify(tobs_list)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        session.close()

@app.route("/api/v1.0/<start>")
def start(start):
    session = Session()
    try:
        # Convert the start date string to a datetime object
        start_date = datetime.strptime(start, '%Y-%m-%d')
        
        # Query to calculate TMIN, TAVG, TMAX for dates >= start_date
        results = (
            session.query(
                func.min(Measurement.tobs),
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            )
            .filter(Measurement.date >= start_date)
            .all()
        )

        # Extract the results
        min_temp, avg_temp, max_temp = results[0]

        # Return the results as JSON
        return jsonify({
            'start_date': start_date.strftime('%Y-%m-%d'),
            'TMIN': min_temp,
            'TAVG': avg_temp,
            'TMAX': max_temp
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
    finally:
        session.close()

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    session = Session()
    try:
        # Convert the start and end date strings to datetime objects
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        
        # Query to calculate TMIN, TAVG, TMAX for dates between start_date and end_date
        results = (
            session.query(
                func.min(Measurement.tobs),
                func.avg(Measurement.tobs),
                func.max(Measurement.tobs)
            )
            .filter(Measurement.date >= start_date)
            .filter(Measurement.date <= end_date)
            .all()
        )

        # Extract the results
        min_temp, avg_temp, max_temp = results[0]

        # Return the results as JSON
        return jsonify({
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'TMIN': min_temp,
            'TAVG': avg_temp,
            'TMAX': max_temp
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)