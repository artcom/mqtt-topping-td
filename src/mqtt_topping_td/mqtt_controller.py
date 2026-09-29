import logging
import platform
import uuid

from mqtt_topping import MqttTopping, TouchDesignerClientAdaptor


class MqttController:
    def __init__(self, ownerComp):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._owner_comp = ownerComp
        client = self._owner_comp.op('mqttclient')
        self._client_adaptor = TouchDesignerClientAdaptor(client)
        self._mqtt_topping = None
        self._is_connected_tmp = False
        self._needs_reconnect = False

    def onInitTD(self):
        self._owner_comp.par.Isconnected = False

    def onDestroyTD(self):
        self._owner_comp.op('mqttclient').par.active = 0

    def OnProjectPreSave(self):
        self._is_connected_tmp = self._owner_comp.par.Isconnected.eval()
        self._owner_comp.par.Isconnected = False

    def OnProjectPostSave(self):
        self._owner_comp.par.Isconnected = self._is_connected_tmp

    def ActivateClient(self):
        self._logger.info("activate")
        self._owner_comp.par.Isconnected = False
        self._mqtt_topping = MqttTopping(self._client_adaptor)
        self.CreateClientId()
        if self._owner_comp.op('mqttclient').par.active == 0:
            self._owner_comp.op('mqttclient').par.active = 1
        else:
            self._owner_comp.op('mqttclient').par.reconnect.pulse()

    def DeactivateClient(self):
        self._logger.info("deactivate")
        self._owner_comp.op('mqttclient').par.active = False
        self._owner_comp.par.Isconnected = False
        self._needs_reconnect = False

    def CreateClientId(self):
        app_id = self._owner_comp.op('app_id')[0, 0]
        hostname = platform.node()
        random_uuid = uuid.uuid4()
        short_uuid = str(random_uuid)[-8:]
        op('mqtt_client_id')[0, 0] = f"{app_id}-{hostname}-{short_uuid}"

    # ------------ MQTT client callbacks ------------

    def OnConnect(self):
        if self._needs_reconnect:
            self._logger.info("reconnected")
            self._needs_reconnect = False
            self._owner_comp.par.Isconnected = True
            self._mqtt_topping.refresh_subscriptions()
            self._logger.info("will refresh subscriptions")
            self._owner_comp.DoCallback('onReconnect')
        else:
            self._logger.info("connected")
            self._owner_comp.par.Isconnected = True
            self._owner_comp.DoCallback('onConnect')

    def OnConnectionFailure(self, error):
        self._logger.error("connection failure %s", error)
        self._owner_comp.par.Isconnected = False
        info = {'error': error}
        self._owner_comp.DoCallback('onConnectFailure', info)

    def OnConnectionLost(self, error):
        self._needs_reconnect = True
        self._logger.error("connection lost %s", error)
        self._owner_comp.par.Isconnected = False
        info = {'error': error}
        self._owner_comp.DoCallback('onConnectionLost', info)

    def OnSubscribe(self):
        self._owner_comp.DoCallback('onSubscribe')

    def OnSubscribeFailure(self, error):
        info = {'error': error}
        self._owner_comp.DoCallback('onSubscribeFailure', info)

    def OnUnsubscribe(self):
        self._owner_comp.DoCallback('onUnsubscribe')

    def OnUnsubscribeFailure(self, error):
        info = {'error': error}
        self._owner_comp.DoCallback('onUnsubscribeFailure', info)

    def OnPublish(self, topic):
        info = {'topic': topic}
        self._owner_comp.DoCallback('onPublish', info)

    def OnMessage(self, topic: str, payload: any):
        self._client_adaptor.on_message(topic, payload)
        info = {'topic': topic, 'payload': payload}
        self._owner_comp.DoCallback('onMessage', info)

    # ------------ MQTT Topping Methods ------------

    def Subscribe(self, topic: str, callback: any, qos: int = 2, parse: bool = True):
        self._mqtt_topping.subscribe(topic, callback, qos=qos, parse=parse)

    def Unsubscribe(self, topic: str, callback: any):
        self._mqtt_topping.subscribe(topic, callback)

    def Publish(self, topic: str, payload: any, encode: bool = True):
        self._mqtt_topping.publish(topic, payload, encode=encode)
