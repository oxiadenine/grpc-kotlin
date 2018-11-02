package grpckotlin

import com.beust.klaxon.json
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeUnit
import io.grpc.ManagedChannelBuilder
import io.grpc.ManagedChannel
import io.grpc.StatusRuntimeException
import java.util.logging.Logger
import java.lang.Exception
import java.util.logging.Level

class FruitStoreClient(host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()

    private val blockingStub: FruitStoreGrpc.FruitStoreBlockingStub
    private val logger: Logger

    init {
        blockingStub = FruitStoreGrpc.newBlockingStub(channel)
        logger = Logger.getLogger(FruitStoreClient::class.java.name)
    }

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    fun allFruits() {
        val request = AllFruitsRequest.newBuilder().build()

        val response = blockingStub.allFruits(request)

        val jsonResponse = if (response.status == Status.SUCCESS) {
            json {
                obj(
                        "ok" to true,
                        "data" to array(response.dataList.map {
                            obj(
                                    "id" to it.id,
                                    "no" to it.no,
                                    "description" to it.description
                            )
                        })
                )
            }
        } else {
            json {
                obj(
                        "ok" to false,
                        "error" to response.error
                )
            }
        }

        logger.info(jsonResponse.toJsonString(true))
    }

    fun oneFruit() {
        val fruitId: Long = 1

        val request = OneFruitRequest.newBuilder()
                .setId(fruitId)
                .build()

        val response = blockingStub.oneFruit(request)

        val jsonResponse = if (response.status == Status.SUCCESS) {
            json {
                obj(
                        "ok" to true,
                        "data" to response.data.let {
                            obj(
                                    "id" to it.id,
                                    "no" to it.no,
                                    "description" to it.description
                            )
                        }
                )
            }
        } else {
            json {
                obj(
                        "ok" to false,
                        "error" to response.error
                )
            }
        }

        logger.info(jsonResponse.toJsonString(true))
    }

    fun newFruit() {
        val fruit = Fruit.newBuilder()
                .setNo("04")
                .setDescription("Mango")
                .build()

        val request = NewFruitRequest.newBuilder()
                .setFruit(fruit)
                .build()

        val response = blockingStub.newFruit(request)

        val jsonResponse = if (response.status == Status.SUCCESS) {
            json {
                obj(
                        "ok" to true,
                        "data" to response.data.let {
                            obj(
                                    "id" to it.id,
                                    "no" to it.no,
                                    "description" to it.description
                            )
                        }
                )
            }
        } else {
            json {
                obj(
                        "ok" to false,
                        "error" to response.error
                )
            }
        }

        logger.info(jsonResponse.toJsonString(true))
    }

    fun editFruit() {
        val fruit = Fruit.newBuilder()
                .setId(13)
                .setNo("04")
                .setDescription("Pineapple")
                .build()

        val request = EditFruitRequest.newBuilder()
                .setFruit(fruit)
                .build()

        val response = blockingStub.editFruit(request)

        val jsonResponse = if (response.status == Status.SUCCESS) {
            json {
                obj(
                        "ok" to true,
                        "data" to response.data.let {
                            obj(
                                    "id" to it.id,
                                    "no" to it.no,
                                    "description" to it.description
                            )
                        }
                )
            }
        } else {
            json {
                obj(
                        "ok" to false,
                        "error" to response.error
                )
            }
        }

        logger.info(jsonResponse.toJsonString(true))
    }

    fun deleteFruit() {
        val fruitId: Long = 13

        val request = DeleteFruitRequest.newBuilder()
                .setId(fruitId)
                .build()

        val response = blockingStub.deleteFruit(request)

        val jsonResponse = if (response.status == Status.SUCCESS) {
            json {
                obj(
                        "ok" to true,
                        "data" to response.data.let {
                            obj(
                                    "id" to it.id,
                                    "no" to it.no,
                                    "description" to it.description
                            )
                        }
                )
            }
        } else {
            json {
                obj(
                        "ok" to false,
                        "error" to response.error
                )
            }
        }

        logger.info(jsonResponse.toJsonString(true))
    }
}

fun main(args: Array<String>) {
    val config = ConfigFactory.load()

    val host = config.getString("grpc.deployment.host")
    val port = config.getInt("grpc.deployment.port")

    val client = FruitStoreClient(host, port)

    client.allFruits()
    client.oneFruit()
    client.newFruit()
    client.editFruit()
    client.deleteFruit()

    client.shutdown()
}
