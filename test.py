import helics as h
import time
import threading

def publisher_federate():
    # Create and configure publisher federate
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Publisher")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, 1.0)
    
    fed = h.helicsCreateValueFederate("Publisher", fedinfo)
    pub = h.helicsFederateRegisterPublication(fed, "net_demand", h.HELICS_DATA_TYPE_STRING, "")
    h.helicsFederateEnterExecutingMode(fed)

    # Publish a test net_demand message at every time step.
    for t in range(10):
        msg = f"{{'test_net_demand': {t}}}"
        h.helicsPublicationPublishString(pub, msg)
        print(f"Publisher: Published {msg} at time {t}")
        # Request the next time step
        h.helicsFederateRequestTime(fed, t+1)
        
    h.helicsFederateFinalize(fed)
    print("[Publisher] Finalized.")

def subscriber_federate():
    # Create and configure subscriber federate
    fedinfo = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreName(fedinfo, "Subscriber")
    h.helicsFederateInfoSetCoreTypeFromString(fedinfo, "zmq")
    h.helicsFederateInfoSetTimeProperty(fedinfo, h.HELICS_PROPERTY_TIME_DELTA, 1.0)
    
    fed = h.helicsCreateValueFederate("Subscriber", fedinfo)
    sub = h.helicsFederateRegisterSubscription(fed, "Publisher/net_demand", "")
    h.helicsFederateEnterExecutingMode(fed)

    # Receive and print the net_demand messages
    for t in range(10):
        granted_time = h.helicsFederateRequestTime(fed, t+1)
        # Wait for the message to be updated
        timeout = 0
        while not h.helicsInputIsUpdated(sub) and timeout < 100:
            time.sleep(0.01)
            timeout += 1

        msg = h.helicsInputGetString(sub)
        print(f"Subscriber: Received {msg} at time {granted_time}")
        
    h.helicsFederateFinalize(fed)
    print("[Subscriber] Finalized.")

# Create the HELICS broker (with 2 federates)
broker = h.helicsCreateBroker("zmq", "", "--federates=2")
time.sleep(1)  # Give the broker time to initialize

# Run publisher and subscriber in separate threads
pub_thread = threading.Thread(target=publisher_federate)
sub_thread = threading.Thread(target=subscriber_federate)

pub_thread.start()
sub_thread.start()

pub_thread.join()
sub_thread.join()

if h.helicsBrokerIsConnected(broker):
    h.helicsBrokerDisconnect(broker)
    h.helicsBrokerFree(broker)

print("Test complete. Broker closed.")
