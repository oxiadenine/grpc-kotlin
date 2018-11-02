package grpckotlin

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import grpckotlin.entities.FruitEntity
import grpckotlin.entities.Fruits
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.IOException
import java.util.logging.Logger

class FruitStoreServer(private val config: Config) {
    private val logger: Logger
    private val server: Server

    init {
        val port = this.config.getInt("grpc.deployment.port")

        server = ServerBuilder.forPort(port)
                .addService(FruitStoreImpl())
                .build()
        logger = Logger.getLogger(FruitStoreServer::class.java.name)
    }

    @Throws(IOException::class)
    fun start() {
        val databaseUrl = this.config.getString("grpc.database.url")
        val databaseDriver = this.config.getString("grpc.database.driver")
        val databaseUser = this.config.getString("grpc.database.user")
        val databasePassword = this.config.getString("grpc.database.password")

        Database.connect(databaseUrl, databaseDriver, databaseUser, databasePassword)

        transaction {
            SchemaUtils.create(Fruits)
        }

        server.start()

        logger.info("Server started, listening on ${this.server.port}")

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down")

                this@FruitStoreServer.stop()

                System.err.println("*** server shut down")
            }
        })
    }

    fun stop() {
        this.server.shutdown()
    }

    @Throws(InterruptedException::class)
    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    internal class FruitStoreImpl : FruitStoreGrpc.FruitStoreImplBase() {
        override fun allFruits(request: AllFruitsRequest?, responseObserver: StreamObserver<AllFruitsResponse>?) {
            val fruits = transaction {
                return@transaction FruitEntity.all().map {
                    Fruit.newBuilder().apply {
                        id = it.id.value
                        no = it.no
                        description = it.description
                    }.build()
                }.toList()
            }

            val responseBuilder = AllFruitsResponse.newBuilder()

            responseBuilder.status = Status.SUCCESS
            responseBuilder.addAllData(fruits)

            responseObserver?.onNext(responseBuilder.build())
            responseObserver?.onCompleted()
        }

        override fun oneFruit(request: OneFruitRequest?, responseObserver: StreamObserver<OneFruitResponse>?) {
            val fruitId = request?.id!!

            val fruitEntity = transaction {
                return@transaction FruitEntity.findById(fruitId)
            }

            val responseBuilder = OneFruitResponse.newBuilder()

            if (fruitEntity != null) {
                val fruit = Fruit.newBuilder().apply {
                    id = fruitEntity.id.value
                    no = fruitEntity.no
                    description = fruitEntity.description
                }.build()

                responseBuilder.status = Status.SUCCESS
                responseBuilder.data = fruit
            } else {
                responseBuilder.status = Status.ERROR
                responseBuilder.error = "Fruit does not exists"
            }

            responseObserver?.onNext(responseBuilder.build())
            responseObserver?.onCompleted()
        }

        override fun newFruit(request: NewFruitRequest?, responseObserver: StreamObserver<NewFruitResponse>?) {
            var fruit = request?.fruit!!

            var fruitEntity = transaction {
                return@transaction FruitEntity.find { Fruits.no eq fruit.no }
                        .toList()
                        .singleOrNull()
            }

            val responseBuilder = NewFruitResponse.newBuilder()

            if (fruitEntity == null) {
                fruitEntity = transaction {
                    return@transaction FruitEntity.new {
                        no = fruit.no
                        description = fruit.description
                    }
                }

                fruit = Fruit.newBuilder().apply {
                    id = fruitEntity.id.value
                    no = fruitEntity.no
                    description = fruitEntity.description
                }.build()

                responseBuilder.status = Status.SUCCESS
                responseBuilder.data = fruit
            } else {
                responseBuilder.status = Status.ERROR
                responseBuilder.error = "Fruit already exists"
            }

            responseObserver?.onNext(responseBuilder.build())
            responseObserver?.onCompleted()
        }

        override fun editFruit(request: EditFruitRequest?, responseObserver: StreamObserver<EditFruitResponse>?) {
            var fruit = request?.fruit!!

            val fruitEntity = transaction {
                return@transaction FruitEntity.findById(fruit.id)
            }

            val responseBuilder = EditFruitResponse.newBuilder()

            if (fruitEntity != null) {
                transaction {
                    fruitEntity.no = fruit.no
                    fruitEntity.description = fruit.description
                }

                fruit = Fruit.newBuilder().apply {
                    id = fruitEntity.id.value
                    no = fruitEntity.no
                    description = fruitEntity.description
                }.build()

                responseBuilder.status = Status.SUCCESS
                responseBuilder.data = fruit
            } else {
                responseBuilder.status = Status.ERROR
                responseBuilder.error = "Fruit does not exists"
            }

            responseObserver?.onNext(responseBuilder.build())
            responseObserver?.onCompleted()
        }

        override fun deleteFruit(request: DeleteFruitRequest?, responseObserver: StreamObserver<DeleteFruitResponse>?) {
            val fruitId = request?.id!!

            val fruitEntity = transaction {
                return@transaction FruitEntity.findById(fruitId)
            }

            val responseBuilder = DeleteFruitResponse.newBuilder()

            if (fruitEntity != null) {
                transaction {
                    fruitEntity.delete()
                }

                val fruit = Fruit.newBuilder().apply {
                    id = fruitEntity.id.value
                    no = fruitEntity.no
                    description = fruitEntity.description
                }.build()

                responseBuilder.status = Status.SUCCESS
                responseBuilder.data = fruit
            } else {
                responseBuilder.status = Status.ERROR
                responseBuilder.error = "Fruit does not exists"
            }

            responseObserver?.onNext(responseBuilder.build())
            responseObserver?.onCompleted()
        }
    }
}

fun main(args: Array<String>) {
    val server = FruitStoreServer(ConfigFactory.load())

    server.start()
    server.blockUntilShutdown()
}
