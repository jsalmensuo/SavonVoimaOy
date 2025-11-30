import datetime as dt
from fmiopendata.wfs import download_stored_query

def weatherApi():
    """Fetch the weather data using FMI API."""
    # Generate time window (last 1 hour)
    end_time = dt.datetime.utcnow()
    start_time = end_time - dt.timedelta(hours=1)

    start_time = start_time.isoformat(timespec="seconds") + "Z"
    end_time = end_time.isoformat(timespec="seconds") + "Z"

    # Fetch data from FMI API (FMI Open Data Service)
    obs = download_stored_query(
        "fmi::observations::weather::multipointcoverage",
        args=["bbox=18,55,35,75",  # Bounding box for the area (latitude, longitude)
              "starttime=" + start_time,
              "endtime=" + end_time]
    )

    # Get the latest observation time step
    latest_tstep = max(obs.data.keys())  # Get the latest observation time step
    return obs.data[latest_tstep]  # Return the data
