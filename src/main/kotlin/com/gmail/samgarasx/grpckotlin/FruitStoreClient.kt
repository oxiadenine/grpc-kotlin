package com.gmail.samgarasx.grpckotlin

import java.util.concurrent.TimeUnit
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannel
import java.util.logging.Logger
import io.grpc.StatusRuntimeException
import java.util.logging.Level


class FruitStoreClient internal constructor(channelBuilder: ManagedChannelBuilder<*>) {
    private val channel: ManagedChannel = channelBuilder.build()
    private val blockingStub: FruitStoreGrpc.FruitStoreBlockingStub

    constructor(host: String, port: Int) : this(ManagedChannelBuilder.forAddress(host, port)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext(true))

    init {
        blockingStub = FruitStoreGrpc.newBlockingStub(channel)
    }

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    fun addFruit(fruit: Fruit) {
        logger.info("Will try to add fruit with description=${fruit.description} ...")

        val request = AddRequest.newBuilder()
                .setFruit(fruit)
                .build()

        val response: AddResponse
        try {
            response = blockingStub.addFruit(request)
        } catch (e: StatusRuntimeException) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.status)
            return
        }

        if (response.status == Status.SUCCESS)
            logger.info("Fruit added with id=${response.result.id}")
        else
            logger.warning(response.error.message)
    }

    fun selectFruits(query: String) {
        logger.info("Will try to select fruits with description=$query ...")

        val request = SelectRequest.newBuilder()
                .setQuery(query)
                .build()

        val response: SelectResponse
        try {
            response = blockingStub.selectFruits(request)
        } catch (e: StatusRuntimeException) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.status)
            return
        }

        if (response.status == Status.SUCCESS) {
            logger.info("Selected fruits:")
            response.resultList.forEach {
                logger.info("Fruit with description=${it.fruit.description}")
            }
        } else
            logger.warning(response.error.message)
    }

    companion object {
        private val logger = Logger.getLogger(FruitStoreClient::class.java.name)

        @Throws(Exception::class)
        @JvmStatic fun main(args: Array<String>) {
            val client = FruitStoreClient("localhost", 50051)
            try {
                var fruit = Fruit.newBuilder()
                        .setId(0)
                        .setNo("003")
                        .setDescription("Strawberry")
                        .build()

                client.addFruit(fruit)

                fruit = Fruit.newBuilder()
                        .setId(0)
                        .setNo("004")
                        .setDescription("Lemon")
                        .build()

                client.addFruit(fruit)

                client.selectFruits("a")
            } finally {
                client.shutdown()
            }
        }
    }
}
