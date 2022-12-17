#!/usr/bin/python
# -*- coding: UTF-8 -*-

#===============================================================================
# C.A.S.A. - Corsican Automation Systems & Applications
# (c) 2011 by Toussaint OTTAVI, bc-109 Soft (t.ottavi@bc-109.com) 
#===============================================================================
#
#   mqttmon : 
#     A simple console mqtt monitor, subscribes to all topics, and display
#     messages in ANSI color.
#
#===============================================================================
#
#    This is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#===============================================================================

#===============================================================================
# Version numbering   
#===============================================================================

Version = "0.1"
VersionDate = "11/12/2022"

# History :
# - v0.1 : Initial / Git Clean

#===============================================================================
# Imports 
#===============================================================================

print ("Importing modules...")

import sys, signal, traceback

from twisted.internet.error import CannotListenError
from twisted.internet import reactor, task

from twisted.internet.defer       import inlineCallbacks, DeferredList
from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet.endpoints   import clientFromString
from mqtt.client.factory import MQTTFactory


################################################################################
#                                                                              #
#                                MQTT CLASS                                    #
#                                                                              # 
################################################################################

MQTT_BROKER = "tcp:192.168.108.2:1883"

#==============================================================================
# MQTT Service
#==============================================================================

class MQTTService(ClientService):

    def __init(self, endpoint, factory):
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())


    def startService(self):
        print("starting MQTT Client Publisher Service")
        # invoke whenConnected() inherited method
        self.whenConnected().addCallback(self.connectToBroker)
        ClientService.startService(self)


    @inlineCallbacks
    def connectToBroker(self, protocol):
        '''
        Connect to MQTT broker
        '''
        self.protocol                 = protocol
        self.protocol.onPublish       = self.onPublish
        self.protocol.onDisconnection = self.onDisconnection
        # We are issuing 3 publish in a row
        # if order matters, then set window size to 1
        # Publish requests beyond window size are enqueued
        self.protocol.setWindowSize(3) 
        
        # self.task = task.LoopingCall(self.publish)
        # self.task.start(5.0)

        try:
            print('--- Connecting to broker for subscriptions')
            yield self.protocol.connect("mqtt-monitor", keepalive=60)
            yield self.subscribe()
        except Exception as e:
            print("Exception connecting to %s for subscribing" %(MQTT_BROKER))
        else:
            print("Connected to %s for subscriptions" %(MQTT_BROKER))
        print('')
            

    def subscribe(self):

        def _logFailure(failure):
            print("Subscription failure : %s" %(failure.getErrorMessage()))
            return failure

        def _logGrantedQoS(value):
            print("Subscription GrantedQOS response %s" %(value))
            return True

        def _logAll(*args):
            print("All subscriptions complete, args=%s" % (args))

        print ("Subscribe to all topics")
        d1 = self.protocol.subscribe("#", 2 )
        d1.addCallbacks(_logGrantedQoS, _logFailure)

        dlist = DeferredList([d1], consumeErrors=True)
        dlist.addCallback(_logAll)
        return dlist


    def onDisconnection(self, reason):
        '''
        get notfied of disconnections
        and get a deferred for a new protocol object (next retry)
        '''
        print(" Connection was lost, reason : %s" %(reason))
        self.whenConnected().addCallback(self.connectToBroker)



    def onPublish(self, topic, payload, qos, dup, retain, msgId):
        '''
        Callback Receiving messages from publisher
        '''
        #print ("Receiving message : %s" %(payload))
        print(DisplayMQTTTwoLinesColor (topic, payload, qos, dup, retain, msgId))




################################################################################
#                                                                              #
#                             DISPLAY PROCEDURES                               #
#                                                                              # 
################################################################################

#===============================================================================
# ANSI Color definitions 
#===============================================================================

# Reset
ANSI_reset="\033[0;39;49m"

# colours bold / bright
ANSI_black="\033[01;30m"    #: Black and bold.
ANSI_red="\033[01;31m"      #: Red and bold.
ANSI_green="\033[01;32m"    #: Green and bold.
ANSI_yellow="\033[01;33m"   #: Yellow and bold.
ANSI_blue="\033[01;34m"     #: Blue and bold.
ANSI_magenta="\033[01;35m"  #: Magenta and bold.
ANSI_cyan="\033[01;36m"     #: Cyan and bold.
ANSI_white="\033[01;37m"    #: White and bold.

# colors not bold/brignt 
ANSI_darkblack="\033[0;39;49m\033[02;30m"   #: Black and not bold.
ANSI_darkred="\033[0;39;49m\033[02;31m"     #: Red and not bold.
ANSI_darkgreen="\033[0;39;49m\033[02;32m"   #: Green and not bold.
ANSI_darkyellow="\033[0;39;49m\033[02;33m"  #: Yellow and not bold.
ANSI_darkblue="\033[0;39;49m\033[02;34m"    #: Blue and not bold.
ANSI_darkmagenta="\033[0;39;49m\033[02;35m" #: Magenta and not bold.
ANSI_darkcyan="\033[0;39;49m\033[02;36m"    #: Cyan and not bold.
ANSI_darkwhite="\033[0;39;49m\033[02;37m"   #: White and not bold.

# Background colors : not very useful
ANSI_Black="\033[40m"    #: Black background
ANSI_Red="\033[41m"      #: Red background
ANSI_Green="\033[42m"    #: Green background
ANSI_Yellow="\033[43m"   #: Yellow background
ANSI_Blue="\033[44m"     #: Blue background
ANSI_Magenta="\033[45m"  #: Magenta background
ANSI_Cyan="\033[46m"     #: Cyan background
ANSI_White="\033[47m"    #: White background


#==============================================================================
# Print MQTT message on two lines with ANSI colors
#==============================================================================

def DisplayMQTTTwoLinesColor (topic, payload, qos, dup, retain, msgId):
  
  rst = ANSI_darkwhite          # Default color
  s = ''
  if True: #try:                                                  
        
    # message type : MQTT
    col = ANSI_magenta
    tmp = rst + '[' + col + "MQTT" + rst + '] '
    s = s + tmp
    
    # Topic
    col = ANSI_white
    tmp =       '[Topic=' + col + topic + rst + '] '
    tmp = tmp + '[QoS=' + col + str(qos) + rst + '] '
    tmp = tmp + '[Dup=' + col + str(dup) + rst + '] '
    tmp = tmp + '[Retain=' +  col + str(retain) + rst + '] ' 
    tmp = tmp + '[MessageId=' + col + str(msgId) + rst + ']'
    s = s + tmp  
      
    # New line  
    s = s + '\n'
    
    # Payload 
    colpay = ANSI_blue   
    payload2=payload.decode("UTF-8")
    pays = ' ' + colpay + payload2 + rst 
    s = s + pays
    
    # New line      
    s = s + '\n'
         
  else: #except:
    s = s + ANSI_red + "<Error formatting message for display>" + rst
  
  return s 


################################################################################
#                                                                              #
#                                MAIN PROGRAM                                  #
#                                                                              # 
################################################################################

if __name__ == '__main__':
  
  print("MQTT Monitor v%s (%s) (c) Toussaint OTTAVI, TK1BI, bc-109 Soft" % (Version, VersionDate))
  
  
  #===============================================================================
  # Startup 
  #===============================================================================
  
  
  try:
      
    # Initialize MQTT class
  
    mqtt_factory      = MQTTFactory(profile=MQTTFactory.SUBSCRIBER)
    mqtt_endpoint     = clientFromString(reactor, MQTT_BROKER)
    mqtt_service      = MQTTService(mqtt_endpoint, mqtt_factory)
    
    # Prepare tasks for Reactor loop 
      
    print('MQTT services starting')    
    mqtt_service.startService()
    
    # Main Reactor loop
      
    reactor.run()
    
    # End
    
    print ("Program terminated.")
        
        
  #=============================================================================== 
  # Exceptions 
  #===============================================================================
  
  # except :
  #   msg = 'Exception : Port %d already in use ?' % xplmon.port
  #   print (msg)
  #
  except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    msg = "Exception : %s" % (exc_value) 
    trace = traceback.format_tb(exc_traceback)
    print (msg)
    print (trace)     
  
  
  
  
  
