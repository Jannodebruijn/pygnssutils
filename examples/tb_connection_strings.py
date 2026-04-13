from pygnssutils.thingsboard import ConnectionParameters


if __name__ == "__main__":
    # Example HTTP connection string
    http_connstr = (
        "http://8ny9n4lkdjqtga4uegfq@spoorwiel-vm-02.westeurope.cloudapp.azure.com:8080"
    )
    cp = ConnectionParameters.from_connection_string(http_connstr)
    print(cp)

    # Example MQTT connection string with username and password and TLS disabled
    mqtt_connstr = (
        "mqtt://4esmhmewt4rs9ngaxqfk:1g6c8acce90l14au8rks@spoorwiel-vm-02.westeurope.cloudapp.azure.com:1883"
        "?client_id=p4xj5dxm6yg8gyti50n2&tls=false"
    )
    cp = ConnectionParameters.from_connection_string(mqtt_connstr)
    print(cp)

    # Example MQTT connection string with access token
    mqtt_connstr2 = (
        "mqtt://4esmhmewt4rs9ngaxqfk@spoorwiel-vm-02.westeurope.cloudapp.azure.com:1883"
    )
    cp = ConnectionParameters.from_connection_string(mqtt_connstr2)
    print(cp)

    # Example MQTT connection string with TLS enabled (w/o username/password)
    mqtt_connstr3 = (
        "mqtt://spoorwiel-vm-02.westeurope.cloudapp.azure.com:1883"
        "?client_id=p4xj5dxm6yg8gyti50n2&tls=true&ca_certs=/path/ca.pem&cert_file=/path/cert.pem&key_file=/path/key.pem"
    )
    cp = ConnectionParameters.from_connection_string(mqtt_connstr3)
    print(cp)

    # Example MQTT connection string with client_id only
    mqtt_connstr4 = "mqtt://spoorwiel-vm-02.westeurope.cloudapp.azure.com:1883?client_id=p4xj5dxm6yg8gyti50n2"
    cp = ConnectionParameters.from_connection_string(mqtt_connstr4)
    print(cp)
