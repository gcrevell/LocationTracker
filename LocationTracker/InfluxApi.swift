//
//  Created by David G. Simmons on 2/21/19.
//  Copyright © 2019 David G. Simmons. All rights reserved.
//

import Foundation

public typealias CompletionHandler = (Data?, URLResponse?, Error?) -> Void

/*
 * Data Precision formats
 */
public enum DataPrecision {
    case s, ms, us
}

public enum UrlProtocol: String {
    case http = "http"
    case https = "https"
}

public class InfluxApi {
    // transmission protocol.
    var _protocol: UrlProtocol = .http
    // server address
    var _server: String
    // server port
    var _port: UInt16? = 8086
    // Data database
    var _database: String
    // timestamp precision
    var _precision: DataPrecision = .s
    // batch of datapoints for batch processing
    var _multiPoint:[Influx] = []

    var retMess: String = ""

    /**
     * Configure an influxDB instance with all required values
     * @param server InfluxDB v2 server to use
     * @param port Server port
     * @param database Database to use -- MUST already exist!
     */
    public init(server: String, port: UInt16?, database: String) {
        self._server = server
        self._port = port
        self._database = database
    }

    /**
     * Add a data point to a batch to be written later.
     * @param point an Influx data Point to add to a batch
     */
    public func prepare(point: Influx) {
        self._multiPoint.append(point)
    }

    /**
     *
     * @param server Set the server address
     */
    public func set(server: String) {
        self._server = server
    }

    /*
     * @param port Set server port. default is 8086. Set to nil to not use a port number
     */
    public func set(port: UInt16?) {
        self._port = port
    }

    /**
     * @param database Set the  database to use -- MUST already exist
     */
    public func set(database: String) {
        self._database = database
    }

    /**
     * @param proto Set the protocol to either http or https. Default is http
     **/
    public func set(protocol value: UrlProtocol) {
        self._protocol = value
    }

    /**
     * @param precision set the timestamp precision to use
     */
    public func set(precision: DataPrecision) {
        self._precision = precision
    }

    /**
     * Return the fully formed URL as a string including all options.
     */
    func getConfig() -> String {
        if let port = _port{
            return "\(_protocol)://\(_server):\(port)/write?db=\(_database)&precision=\(_precision)"
        } else {
            return "\(_protocol)://\(_server)/write?db=\(_database)&precision=\(_precision)"
        }
    }

    /**
     * Write the batch of prepared data points to the database.
     */
    public func writeBatch(completionHandler: @escaping CompletionHandler) {
        guard self._multiPoint.count > 1 else { return }
        var points = ""
        for data in _multiPoint {
            let time = getTimestamp(from: data._time)
            points = "\(points)\(data.toString()) \(time)\n"
        }
        points.removeLast()
        _multiPoint.removeAll()
        let postUrl = URL(string: self.getConfig())
        var request = URLRequest(url: postUrl!)
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        request.httpMethod = "POST"
        request.httpBody = points.data(using: .utf8)
        let task = URLSession.shared.dataTask(with: request, completionHandler: completionHandler)
        task.resume()
    }

    public func writeBatch() {
        self.writeBatch { (data, response, error) in
            guard let _ = data,
                let response = response as? HTTPURLResponse,
                error == nil else {
                    DispatchQueue.main.async {
                        print("Error: ", error ?? "Unknown error")
                    }
                    return
            }
            guard (200 ... 299) ~= response.statusCode else {
                DispatchQueue.main.async {
                    print("statusCode should be 2xx, but is \(response.statusCode)")
                }
                return
            }
            DispatchQueue.main.async {
                print("InfluxDB response: \(response.statusCode)")
            }
        }
    }

    /**
     * @param Single Influx Datapoint to write to the db
     */
    public func writeSingle(dataPoint: Influx) {
        let postUrl = URL(string: self.getConfig())
        var request = URLRequest(url: postUrl!)
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        request.httpMethod = "POST"
        request.httpBody = "\(dataPoint.toString()) \(getTimestamp())".data(using: .utf8)
        let task = URLSession.shared.dataTask(with: request) { data, response, error in
            guard let _ = data,
                let response = response as? HTTPURLResponse,
                error == nil else {                                              // check for fundamental networking error
                    DispatchQueue.main.async {
                        print("Error:  \(String(describing: error)) Unknown error")
                        self.retMess = "Error:  \(String(describing: error)) Unknown error"
                    }
                    return
            }
            guard (200 ... 299) ~= response.statusCode else {                    // check for http errors
                DispatchQueue.main.async {
                    print("statusCode should be 2xx, but is \(response.statusCode)")
                    self.retMess = "statusCode should be 2xx, but is \(response.statusCode)"
                    print("response = \(response.statusCode)")
                }
                return
            }
            DispatchQueue.main.async {
                self.retMess = "InfluxDB response: \(response.statusCode)"
                print("InfluxDB response: \(response.statusCode)")
            }

        }
        task.resume()
    }

    // construct the timestamp based on the configured precision.
    // only supporting seconds, milliseconds and microseconds now
    func getTimestamp(from date: Date? = nil) -> String {
        if let date = date {
            let timeMultiplier: Double = {
                switch self._precision {
                case .s:
                    return 1.0
                case .ms:
                    return 1000.0
                case .us:
                    return 1000000.0
                }
            }()
            let dateSeconds = date.timeIntervalSince1970 * timeMultiplier

            return String(dateSeconds.rounded())
        }

        var time = timeval()
        gettimeofday(&time, nil)
        switch _precision {
        case DataPrecision.s:
            return "\(time.tv_sec)"
        case DataPrecision.us:
            return "\(time.tv_sec)\(time.tv_usec)"
        default:
            var ms = "\(time.tv_sec)\(time.tv_usec)"
            ms.removeLast()
            ms.removeLast()
            ms.removeLast()

            return ms

        }
    }
}

/*
 * Influx data point object.
 */
public class Influx {
    // measurement to insert into
    var _measurement: String = ""
    // tags
    var _tag: [String:String] = [:]
    // values
    var _value: [String:Any] = [:]
    // time
    var _time: Date?

    /*
     * Create a new data point for a given measurement
     * @param measurement to store into
     */
    public init(measurement: String) {
        self._measurement = measurement
    }

    /*
     * Add a tag to a data point
     * @param name Name of the tag
     * @param value Tag value
     */
    public func addTag(name: String, value: String) {
        self._tag[name] = value
    }

    /**
     * Add an integer value to the measurment point
     *
     * @param value The name of the value
     * @param
     */
    public func add(field name: String, value: Int) {
        self._value.updateValue(value, forKey: name)
    }

    public func add(field name: String, value: Float) {
        self._value.updateValue(value, forKey: name)
    }

    public func add(field name: String, value: Bool) {
        self._value.updateValue(value, forKey: name)
    }

    public func add(field name: String, value: String) {
        self._value.updateValue(value, forKey: name)
    }

    public func set(time: Date) {
        self._time = time
    }

    /*
     * Get the point's tags as a key=value string
     * @return string of comma-separataed key=value pairs
     */
    public func getTags() -> String {
        var tagString = ""
        for (key, value) in self._tag {
            let ts = "\(tagString)\(key)=\(value)"
            tagString = "\(ts),"
        }
        tagString.removeLast()
        return tagString
    }

    /*
     * Get the point's data values
     * @return comma-separated key=value pairs
     */
    public func getValues() -> String {
        var valString = " "
        for (key, value) in self._value {
            valString = "\(valString)\(key)=\(value),"
        }
        valString.removeLast()
        return valString
    }

    /*
     * return the data point as a Line Protocol formatted string.
     */
    public func toString() -> String {
        let s = "\(_measurement),\(getTags())\(getValues())"
        return s
    }


}
