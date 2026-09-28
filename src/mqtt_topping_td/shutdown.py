def onExit():
    op('mqttclient').par.active = False


def onProjectPreSave():
    op.MQTT_TOPPING.OnProjectPreSave()


def onProjectPostSave():
    op.MQTT_TOPPING.OnProjectPostSave()
