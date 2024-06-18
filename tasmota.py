try:
    import collections.abc as collections
except ImportError:  # Python <= 3.2 including Python 2
    import collections

errmsg = ""
try:
    import Domoticz
except Exception as e:
    errmsg += "Exception: Domoticz core start error: "+str(e)
    
import json
import binascii
import re
import typing
import json
import time

tasmotaDebug = True


# Decide if tasmota.py debug messages should be displayed if domoticz debug is enabled for this plugin
def setTasmotaDebug(flag):
    global tasmotaDebug
    tasmotaDebug = flag


# Replaces Domoticz.Debug() so tasmota related messages can be turned off from plugin.py
def Debug(msg, OnOff=None):
    if OnOff=='On': setTasmotaDebug(True)
    if tasmotaDebug or OnOff=='One':
        Domoticz.Debug(msg)
    if OnOff=='Off': setTasmotaDebug(False)


 # Configuration Helpers
def getConfigItem(Key=None, Default=None):
    Debug('\ntasmota:getConfigItem({}, {})'.format(repr(Key), repr(Default)))#, 'One')
    Value = Default
    try:
        Config = Domoticz.Configuration() or dict()
        if (Key != None):
            Value = eval(Config[Key]) # only return requested key if there was one
    except KeyError:
        Value = Default
    except Exception as inst:
        Domoticz.Error("Domoticz.Configuration read failed: '"+str(inst)+"'")
    Debug('===> {}'.format(repr(Value)))#, 'One')
    return Value
        
def setConfigItem(Key=None, Value=None, dontShow=False):
    Debug('\ntasmota:setConfigItem({}, {})'.format(repr(Key), repr(Value)), Value and not dontShow and 'One')
    try:
        Config = Domoticz.Configuration() or dict()
        if (Key != None):
            Config[Key] = repr(Value)
            Domoticz.Configuration(Config)
    except Exception as inst:
        Domoticz.Error("Domoticz.Configuration operation failed: '"+str(inst)+"'")
    return Config


# Handles incoming Tasmota messages from MQTT or Domoticz commands for Tasmota devices
class Handler:
    def __init__(self, subscriptions, prefixes, tasmotaDevices, mqttClient, devices):
        Debug("Handler::__init__(prefixes: {}, subs: {})".format(prefixes, repr(subscriptions)))

        if errmsg != "":
            Domoticz.Error(
                "Handler::__init__: Domoticz Python env error {}".format(errmsg))

        self.prefix = [None] + prefixes
        self.tasmotaDevices = tasmotaDevices
        self.subscriptions = subscriptions
        self.mqttClient = mqttClient

        # I don't understand variable (in)visibility
        global Devices
        Devices = devices


    def debug(self, val):
        setTasmotaDebug(val)


    # Translate domoticz command to tasmota mqtt command(s?)
    def onDomoticzCommand(self, Unit, Command, Level, Color):
        Debug("\nHandler::onDomoticzCommand: Unit: {}, Command: {}, Level: {}, Color: {}".format(
            Unit, Command, Level, Color))

        ID  = getConfigItem('Unit'+str(Unit)+":ID")
        if not ID or not self.mqttClient: return False

        msg = json.dumps(updateDevices('doeterniettoe', 'DOMOTICZ', { ID: "",  Command: Level, "Color": Color }))

        Debug('msg = {}'.format(repr(msg)))

        topic   = "cmnd/" + ID.split('&')[0] + "/json"

        try:
            self.mqttClient.publish(topic, msg)
        except Exception as e:
            Domoticz.Error("Handler::onDomoticzCommand Exception: {}".format(str(e)))
            return False

        return True



    # Subscribe to our topics
    def onMQTTConnected(self):
        subs = []
        for topic in self.subscriptions:
            topic = topic.replace('%topic%', '+')
            subs.append(topic.replace('%prefix%', self.prefix[2]) + '/+')
            subs.append(topic.replace('%prefix%', self.prefix[3]) + '/+')
        Debug('\nHandler::onMQTTConnected: Subscriptions: {}'.format(repr(subs)), 'One')
        self.mqttClient.subscribe(subs)
        self.mqttClient.publish('cmnd/tasmotas/status0', '')     # call al tasmotas to identify themselves


    # Process incoming MQTT messages from Tasmota devices
    # Call Update{subtopic}Devices() if it is potentially one of ours
    def onMQTTPublish(self, topic, message):
        # self.topics: 'INFO1', 'STATE', 'SENSOR', 'RESULT', 'STATUS', 'STATUS5', 'STATUS8', 'STATUS11', 'ENERGY'
        # self.subscriptions: ['%prefix%/%topic%', '%topic%/%prefix%'] 
        # Check if we handle this topic tail at all (hardcoded list SENSOR, STATUS, ...)
        subtopics = topic.split('/')
        tail = subtopics[-1]
 
        # Different Tasmota devices can have different FullTopic patterns.
        # All FullTopic patterns we care about are in self.subscriptions (plugin config)
        # Tasmota devices will be identified by a hex hash from FullTopic without %prefix%

        # Identify the subscription that matches our received subtopics
        fulltopic = []
        cmndtopic = []
        for subscription in self.subscriptions:
            patterns = subscription.split('/')
            for subtopic, pattern in zip(subtopics[:-1], patterns):
                if( (pattern not in ('%topic%', '%prefix%', '+', subtopic)) or
                    (pattern == '%prefix%' and subtopic != self.prefix[2] and subtopic != self.prefix[3]) or
                    (pattern == '%topic%' and (subtopic == 'sonoff' or subtopic == 'tasmota')) ):

                    fulltopic = []
                    cmndtopic = []
                    break
                if(pattern != '%prefix%'):
                    fulltopic.append(subtopic)
                    cmndtopic.append(subtopic)
                else:
                    cmndtopic.append(self.prefix[1])
            if fulltopic != []:
                break

        if not fulltopic:
            return True

        fullName = '/'.join(fulltopic)
        cmndName = '/'.join(cmndtopic)

        # fullName should now contain all subtopic parts except for %prefix%es and tail
        # I.e. fullName is uniquely identifying the sensor or button referred by the message
        # setTasmotaDebug('On');
        Debug("\nHandler::onMQTTPublish: device: {}, cmnd: {}, tail: {}, message: {}".format(fullName, cmndName, tail, str(message)), 'Off')

        updateDevices(fullName, tail, message)

        return True




#  * Valid DomoType strings can be found in maptypename(): https://github.com/domoticz/domoticz/blob/development/hardware/plugins/PythonObjects.cpp#L365


# Create a domoticz device from infos extracted out of tasmota STATE tele messages (POWER*)

'''
Berihten komen binnen op een Tasmota_xxx topic
Tasmota_xxx,        SENSOR,     {   Time: xxxx, 
                                    Keuken: { mac: xxxx, Temp: xxx, Hum: xxx, ...}, 
                                    Buiten: { mac: xxxx, Temp: xxxx, Hum: xxxx }, 
                                    ESP32: { Temp: xxxx }, 
                                    ANALOG: { AN1: xxx, AN2: xxx}, 
                                    TempUnit: "C" 
                                }  ====>
Tasmota_xxx,        Time,       xxx     -- niet herkend, genegeerd                                   
Tasmota_xxx,        Keuken,     { mac: xxxx, Temp: xxx, Hum: xxx, ... }     -- Herkend als Temp+Hum of Temp+Hum+Baro die van een extern device af komt
Tasmota_yyy,        Buiten,     { mac: yyyy, Temp: yyyy, Hum: yyyy, ... }   -- Als Temp+Hum niet als een sensor samengevoegd moeten worden kan dit worden omgezet in ===>
Macyyy Temperature, Temperature,xxxx
Macyyy Humidity,    Humidity,   yyyy
Tasmota_xxx ESP32,  Temperature,xxxx
Tasmota_xxx         ANALOG,     { AN1: xxx, AN2: yyy }  ===>
Tasmota_xxx AN1     
'''

class MessageHandlerList(list):
    def handleMessage(self, unitName, msgName, values, handled=[]):
        result  = None
        Debug('MessageHandlerList::handleMessage({}, {}, {}, {}'.format(unitName, msgName, repr(values), repr(handled)))
        for msg,val in ({msgName:values} |                  # First, we check if we can handle msgName.
                    (type(values)==dict and (values | {'msgName':msgName}) or {})).items():  # If we don't recognize msgName, handle all separate parts of values. 
            if msg in handled: continue
            for x in self:
                    #try:
                    m   = re.fullmatch(x.respondsTo, msg)  
                    if m: 
                        h   = []        # handled in this round
                        ourValues   = {msg:val}
                        Debug('Found '+msg + ' ' + repr(val))
                        for also in x.alsoNeeded + x.optional:
                            fullAlso    = also.format(*m.groups()) 
                            if also in x.alsoNeeded: 
                                if not fullAlso in values: Debug(fullAlso + ' not in ' +repr(values));  break
                                h       += [fullAlso]
                            if fullAlso in values:  ourValues   |= {fullAlso:values[fullAlso]}
                        else:   # We only get here if there is no break due to a missing alsoNeeded
                            handled = handled + h
                            result  = x.handle(unitName, m, values, handled, ourValues)
                            if msg==msgName: return result # This needs a comment
                            else: break
                    #except Exception as e:  
                    #Debug('MessageHandlerList::handleMessage exception <<{}>> while evaluating <<{}>>'.format(str(e), str(msg)), 'One')
            else:
                if msg!=msgName and type(val)==dict:  result = self.handleMessage(unitName, 'SubDevices', val | { 'msgName': msg }, handled)
        return result

class MessageHandler:   # Is called by MessageHandlerList if it finds respondsTo and alsoNeeded in its input
    def __init__(self, respondsTo, alsoNeeded=[], optional=[], switchTo=None):
        self.respondsTo = respondsTo
        self.alsoNeeded = alsoNeeded
        self.optional   = optional
        self.switchTo   = switchTo

    def handle(self, unitName, m, values, handled, ourValues):
        Debug('MessageHandler::handle({}, {}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled), repr(ourValues)))#, 'One')
        return self.switchTo and self.switchTo.handleMessage(unitName, 'NewGroup', values | {'OldGroup':m.group()}, handled) or False


class DummyHandler(MessageHandler):
    def handle(self, unitName, m, values, handled, ourValues):
        Debug('DummyHandler({}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled)))#, 'One')
        return True

class DeviceHandler(MessageHandler):
    def __init__(self, respondsTo, alsoNeeded=[], optional=['Battery', 'RSSI'], switchTo=None, typeName=None, updArgs=None, createArgs=dict()):
        super().__init__(respondsTo, alsoNeeded, optional, switchTo)
        self.typeName   = typeName
        self.updArgs    = updArgs
        self.createArgs = createArgs

    def getUnit(self, ID, friendlyName):    # Finds unit for our ID and creates it if it doesn't exist
        unit    = getConfigItem(ID+':Unit', None)
        #Debug('DeviceHandler::getUnit({}, {}, {})'.format(repr(ID), repr(friendlyName), repr(unit)), 'One')
        if unit not in Devices or getConfigItem('Unit'+str(unit)+":ID") != ID:  # Check if there is a device and if the device is the correct one
            setConfigItem(ID+':Unit', None)                                     # Mark previous unit as invalid
            if not self.typeName: Debug('No type name specified for ID ' + ID); return None
            
            for unit in range(1,255):
                if unit not in Devices: break
            else: Debug('getUnit: no device number free', 'One');   return None
            
            Debug('DeviceHandler::getUnit: Create unit of type <<{}>> with ID <<{}>>'.format(self.typeName, ID), 'One')
            Domoticz.Device(Name=str(unit), Unit=unit, DeviceID=ID, TypeName=self.typeName, Used=1).Create()
            if not unit in Devices:
                Debug('DeviceHandler::getUnit failed to create unit of type <<{}>> with ID <<{}>>'.format(self.typeName, ID), 'One')
                return None
            
            setConfigItem('Unit'+str(unit)+":ID", ID)
            Devices[unit].Update(nValue=0, sValue="", Name=friendlyName, SuppressTriggers=True, **self.createArgs),
            Debug('DeviceHandler::getUnit options string: {}'.format(repr(Devices[unit].Options)))
            setConfigItem(ID+':Unit', unit)
        return unit

    def update(self, unitName, unit, values, ID, friendlyName):
        if not self.updArgs or unit not in Devices: return
       
        rssi    = values.get('RSSI')
        if rssi:
            rssi = int((rssi+100)/4) # map the range -100..-60 to 0..10
            rssi = (rssi>10 and 10) or (rssi<0 and 0) or rssi
            oldRSSI     = getConfigItem(ID+':oldRSSI', -1)
            lastUpdate  = getConfigItem(ID+':lastUpdate', 0)
            oldTasmo    = getConfigItem(ID+':oldTasmo', "")
            now         = time.time()
            if rssi<oldRSSI+2 and unitName!=oldTasmo and now-lastUpdate<90: return      # skip update if another stronger receiver has updated in the last 90 seconds
            if rssi<oldRSSI-2 and now-lastUpdate<63: rssi=oldRSSI                       # ignore short dips in RSSI
            else: 
                setConfigItem(ID+':lastUpdate', now, dontShow=True)
                setConfigItem(ID+':oldRSSI', rssi, dontShow=True)
                setConfigItem(ID+':oldTasmo', unitName, dontShow=True)
            #Debug('{}, {}, {}, {}, {}'.format(ID, repr(oldRSSI), repr(rssi), repr(rssi-oldRSSI), repr(now-lastUpdate)), 'One')
        else:
            rssi    = 12    # means 'No RSSI present'
         
        # Do not update the Friendly Name with every 'normal' update. Update it only when there is a Friendly Name update
        #optionalArgs    = ', "Name":"{}", "SignalLevel":{}, "BatteryLevel":{}'.format(friendlyName, rssi, values.get('Battery', 255))
        optionalArgs    = ', "SignalLevel":{}, "BatteryLevel":{}'.format(rssi, values.get('Battery', 255))
        Debug('Updating({}, {},   Image: {})'.format(str(unit), repr(values), Devices[unit].Image))#, 'One')
        Debug(self.updArgs.format(*values.values()) + optionalArgs)#, 'One') 
        Devices[unit].Update(**eval('{' + self.updArgs.format(*values.values()) + optionalArgs + '}')) 
        
    def setName(self, unit, newName):
        Devices[unit].Update(nValue=0, sValue="", Name=newName)     # This will give a warning because of invalid sValue. Ignore it. How else do we update only name?

class SensorDeviceHandler(DeviceHandler):
    def handle(self, unitName, m, values, handled, ourValues):
        Debug('SensorDeviceHandler({}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled)))#, 'One')
        ID              = 'mac' in values and values['mac'].upper() or unitName
        friendlyName    = getConfigItem(ID+':DeviceName', values['msgName'])
        if not 'mac' in values:
            suffix      = ('msgName' in values and ' ' + values['msgName'] or '') + ' ' + m.group()
            ID          = ID + suffix
            friendlyName= friendlyName + suffix

        unit    = self.getUnit(ID, friendlyName)
        self.update(unitName, unit, ourValues, ID, friendlyName)
        return True

class PowerDeviceHandler(DeviceHandler):
    def handle(self, unitName, m, values, handled, ourValues):
        Debug('PowerDeviceHandler({}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled)))
        n               = int(m.groups()[1] or 1)       # POWER is POWER1, PWM is PWM1
        fullName        = m.groups()[0]+str(n)
        ID              = '&'.join([unitName, fullName]) #+ ['{}'.format(also).format(0,n) for also in self.alsoNeeded])
        #Commands        = '&'.join([fullName] + ['{}'.format(also).format(0,n) for also in self.alsoNeeded])
        deviceName      = getConfigItem(unitName+':DeviceName', unitName)
        friendlyNames   = getConfigItem(unitName+':FriendlyName', [])
        friendlyName    = (m.groups()[0]=='POWER' and n<=len(friendlyNames) and friendlyNames[n-1] or deviceName + ' ' + m.groups()[0] + str(n))
        Debug('PowerDeviceHandler("{}", "{}", "{}")'.format(ID, friendlyName, repr(ourValues)))
        unit    = self.getUnit(ID, friendlyName)
        self.update(unitName, unit, ourValues, ID, friendlyName)
        return True

class NameHandler(MessageHandler):
    def handle(self, unitName, m, values, handled, ourValues):
        Debug('NameHandler({}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled)),'On')
        setConfigItem(unitName+':'+m.group(), values[m.group()])
        return True

class BLEReadNameHandler(DeviceHandler):
    def handle(self, unitName, m, values, handled, ourValues):
        Debug('BLEReadNameHandler({}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled)),'On')
        if values['svc']=='0x1800' and values['char']=='0x2a00':
            newName = bytes.fromhex(values['read']).decode('utf-8')
            Debug(newName)
            ID      = values['MAC']
            unit    = self.getUnit(ID, newName)
            if unit:
                self.setName(unit, newName)
        return True

# 13:02:11.956 MQT: tele/tasmota_FBBC2C/BLE = {"BLEOperation":{"opid":"2","stat":"3","state":"DONEREAD","MAC":"A4C1389628F7","svc":"0x1800","char":"0x2a00","read":"4769657A656E6B616D6572"}}
BLEOperationsHandlers = MessageHandlerList([
    BLEReadNameHandler(r'(read)', ['MAC', 'svc', 'char']),
])

BLEHandlers = MessageHandlerList([
    MessageHandler(r'(BLEOperation)',   switchTo=BLEOperationsHandlers  ),
])

sensorDeviceHandlers    = MessageHandlerList([
    SensorDeviceHandler(r'(Temperature)(\d*)', ['Humidity{1}', 'Pressure{1}'],  typeName='Temp+Hum+Baro',   updArgs=' "nValue":0, "sValue":"{0};{1};0;{2};7"'   ),
    #SensorDeviceHandler(r'(Temperature)(\d*)', ['Humidity{1}'],                 typeName='Temp+Hum+Baro',   updArgs=' "nValue":0, "sValue":"{0};{1};0;1030;7"'  ),
    SensorDeviceHandler(r'(Temperature)(\d*)', ['Humidity{1}'],                 typeName='Temp+Hum',        updArgs=' "nValue":0, "sValue":"{0};{1};0"'      ),
    SensorDeviceHandler(r'(A)(\d*)'),
    SensorDeviceHandler(r'(Humidity)(\d*)',                                     typeName='Humidity',        updArgs=' "nValue":round({0}), "sValue":""'    ),
    SensorDeviceHandler(r'(Illuminance)(\d*)'),
    SensorDeviceHandler(r'(Pressure)(\d*)',                                     typeName='Barometer',       updArgs=' "nValue":0, "sValue":"{0};5"' ),
    SensorDeviceHandler(r'(Temperature)(\d*)',                                  typeName='Temperature',     updArgs=' "nValue":0, "sValue":"{0}"'   ),
    SensorDeviceHandler(r'(OBJTMP)(\d*)',                                       typeName='Temperature',     updArgs=' "nValue":0, "sValue":"{0}"'   ),
    SensorDeviceHandler(r'(AMBTMP)(\d*)',                                       typeName='Temperature',     updArgs=' "nValue":0, "sValue":"{0}"'   ),
])

powerDeviceHandlers    = MessageHandlerList([
    PowerDeviceHandler(r'(POWER)(\d*)', ['Channel{1}'], typeName='Dimmer',      updArgs=' "nValue":"{0}"=="ON" and 2 or 0, "sValue":"{1}"'          ),  # so15 1, lamp dimmer, gamma curve
    PowerDeviceHandler(r'(POWER)(\d*)',                 typeName='Switch',      updArgs=' "nValue":"{0}"=="ON" and 1 or 0, "sValue":""'             ),
    PowerDeviceHandler(r'(PWM)(\d+)',                   typeName='Dimmer',      updArgs=' "nValue":"{0}"!="0"  and 2 or 0, "sValue":str(round({0}/10.23))',     createArgs={'Image':7} )   # so15 0, fan icon, linear
])

statusHandlers    = MessageHandlerList([    # Handles Status:
    NameHandler(r'(DeviceName|FriendlyName)')
])

STATUSHandlers    = MessageHandlerList([    # Handles STATUS[0-9]*
    MessageHandler(r'(Status)',                    switchTo=statusHandlers         ),   
    MessageHandler(r'(StatusSNS)',                 switchTo=sensorDeviceHandlers   ),   
    MessageHandler(r'(StatusSTS)',                 switchTo=powerDeviceHandlers    )   
])

# domoticzHandlers zijn een buitenbeentje. respondsTo id het ID, alsoNeeded de _value_ van het Command uit onDomoticzCommand. Beide hebben een waarde die onbenut is.
# Een Domoticz message: 
# DOMOTICZ { "domoticz_XXX&POWER1":"", "On": Level }
# DOMOTICZ { "domoticz_XXX&POWER1":"", "Set Level": Level }

class DomoticzCommandHandler(MessageHandler):
    def __init__(self, respondsTo, cmd, msg):
        super().__init__(respondsTo, cmd)
        self.msg    = msg

    def handle(self, unitName, m, values, handled, ourValues):
        Debug('PowerDeviceHandler({}, {}, {}, {}, {}'.format(unitName, m.group(), repr(values), repr(handled), repr(ourValues)))#,'One')
        g   = m.groups()
        val = list(g) + [*sum(list(zip([*ourValues],ourValues.values())),())][2:]
        Debug('DomoticzCommandHandler::handle val={}'.format(repr(val)), 'On')
        return eval('{' + self.msg.format(*val) + '}')

domoticzHandlers   = MessageHandlerList([
    DomoticzCommandHandler(r'([^&]*)&(POWER)(\d+)',     ['On'],         msg = '"{1}{2}": "{3}"'),               # {0} is tasmota_XXX, {1} is POWER, {2} is 1-32, {3} is 'On', {4} is Level 
    DomoticzCommandHandler(r'([^&]*)&(POWER)(\d+)',     ['Off'],        msg = '"{1}{2}": "{3}"'),               # {0} is tasmota_XXX, {1} is POWER, {2} is 1-32, {3} is 'On', {4} is Level 
    DomoticzCommandHandler(r'([^&]*)&(POWER)(\d+)',     ['Set Level'],  msg = '"Channel{2}": "{4}"'), 
    DomoticzCommandHandler(r'([^&]*)&(PWM)(\d+)',       ['Set Level'],  msg = '"{1}{2}": round({4}*10.23)'),    # Level gaat van 0-100, maar PWM heeft een range van 0-1023
    DomoticzCommandHandler(r'([^&]*)&(PWM)(\d+)',       ['Off'],        msg = '"{1}{2}": 0')                    # 'On' is handled by Set Level. 'Off' remembers last level.
])

topLevelHandlers    = MessageHandlerList([
    MessageHandler(r'(BLE)',            switchTo=BLEHandlers            ),
    MessageHandler(r'(SENSOR)',         switchTo=sensorDeviceHandlers   ),
    MessageHandler(r'(RESULT|STATE)',   switchTo=powerDeviceHandlers    ),
    MessageHandler(r'(STATUS)(\d*)',    switchTo=STATUSHandlers         ),
    MessageHandler(r'(DOMOTICZ)',       switchTo=domoticzHandlers       ),
    DummyHandler(  r'(.*)')     # ignore everything we don't recognize, but claim it, so the contents won't be parsed any further
])

def updateDevices(unitName, itemName, Values):
    return topLevelHandlers.handleMessage(unitName, itemName, Values)

