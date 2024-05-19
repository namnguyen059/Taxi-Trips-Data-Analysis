function transform(line) {
    var values = line.split(',');
    var custObj = new Object();
    custObj.ride_id = values[0];
    custObj.point_idx = values[1];
    custObj.latitude = values[2];
    custObj.longitude = values[3];
    custObj.timestamp = values[4];
    custObj.meter_reading = values[5];
    custObj.meter_increment = values[6];
    custObj.ride_status = values[7];
    custObj.passenger_count = values[8];
    var outJson = JSON.stringify(custObj);
    return outJson;
    }