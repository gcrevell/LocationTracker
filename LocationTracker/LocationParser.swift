//
//  LocationParser.swift
//  LocationTracker
//
//  Created by Gabriel Revells on 1/14/20.
//  Copyright © 2020 Gabriel Revells. All rights reserved.
//

import UIKit
import CoreLocation

struct InfluxLocation {
    let latitude: Float
    let longitude: Float
    let altitude: Float

    let time: Date
}

func parse(locations: [CLLocation]) {
    var parsedLocations: [InfluxLocation] = []

    print("Parsing some locations!")

    for location in locations {
        let parsedLocation = InfluxLocation(latitude: Float(location.coordinate.latitude), longitude: Float(location.coordinate.longitude), altitude: Float(location.altitude), time: location.timestamp)
        parsedLocations.append(parsedLocation)
    }

    upload(locations: parsedLocations)
}

func upload(locations: [InfluxLocation]) {
    let api = InfluxApi(server: "192.168.1.9", port: 8086, database: "lyfe")

    let deviceId = UIDevice.current.identifierForVendor?.description

    for location in locations {
        let point = Influx(measurement: "location")
        point.addTag(name: "user", value: "wowza7125")
        point.addTag(name: "device_name", value: "iphone")
        if let deviceId = deviceId {
            point.addTag(name: "device_id", value: deviceId)
        }
        point.add(field: "latitude", value: location.latitude)
        point.add(field: "longitude", value: location.longitude)
        point.add(field: "altitude", value: location.altitude)

        point.set(time: location.time)

        api.prepare(point: point)
    }

    api.writeBatch()
}
