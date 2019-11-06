# Online Event Processing in Scala

Library to implement a resilient online event processing client for Kafka in Scala using FS2 and Cats-Effect. Based on this paper - https://cacm.acm.org/magazines/2019/5/236423-online-event-processing/abstract

# Usage

Implement an OffsetStore[F] and provide it to the EventProcessingClient together with Kafka configuration. The EventProcessingClient will provide a stream of commits from Kafka. In the event of a local failure the client will restart at the last processed offset, which may have been processed by another client.
