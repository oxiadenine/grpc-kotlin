package com.gmail.samgarasx.grpckotlin

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SqlExpressionBuilder.like
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.IOException
import java.sql.SQLException
import java.util.logging.Logger

class FruitStoreServer(private val port: Int) {
    private val logger: Logger
    private var server: Server? = null

    init {
        this.server = ServerBuilder.forPort(port)
                .addService(FruitStoreImpl())
                .build()
        this.logger = Logger.getLogger(FruitStoreServer::class.java.name)
    }

    @Throws(IOException::class)
    fun start() {
        this.initializeDatabase()

        this.server!!.start()

        logger.info("Server started, listening on " + port)
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
        if (this.server != null) {
            this.server!!.shutdown()
        }
    }

    @Throws(InterruptedException::class)
    fun blockUntilShutdown() {
        if (this.server != null) {
            this.server!!.awaitTermination()
        }
    }

    internal class FruitStoreImpl : FruitStoreGrpc.FruitStoreImplBase() {
        override fun addFruit(request: AddRequest?, responseObserver: StreamObserver<AddResponse>?) {
            val fruit = request?.fruit
            if (fruit!!.isInitialized) {
                val responseBuilder = AddResponse.newBuilder()
                try {
                    val fruitId = transaction {
                        return@transaction Fruits.insert {
                            it[no] = fruit.no
                            it[description] = fruit.description
                        } get Fruits.id
                    }

                    responseBuilder.status = Status.SUCCESS
                    responseBuilder.result = Result.newBuilder()
                            .setId(fruitId)
                            .build()
                } catch (e: SQLException) {
                    responseBuilder.status = Status.ERROR
                    responseBuilder.error = responseBuilder.error.toBuilder()
                            .setCode(e.errorCode)
                            .setMessage(e.message)
                            .build()
                } finally {
                    responseObserver?.onNext(responseBuilder.build())
                    responseObserver?.onCompleted()
                }
            }
        }

        override fun selectFruits(request: SelectRequest?, responseObserver: StreamObserver<SelectResponse>?) {
            val query = request?.query
            if (!query.isNullOrEmpty()) {
                val responseBuilder = SelectResponse.newBuilder()
                try {
                    val fruits = transaction {
                        return@transaction Fruits.select(Fruits.description like "%$query%")
                                .map {
                                    Fruit.newBuilder()
                                            .setId(it[Fruits.id])
                                            .setNo(it[Fruits.no])
                                            .setDescription(it[Fruits.description])
                                            .build()
                                }
                                .toList()
                    }

                    responseBuilder.status = Status.SUCCESS
                    fruits.forEach {
                        val result = Result.newBuilder().setFruit(it).build()
                        responseBuilder.addResult(result)
                    }
                } catch (e: SQLException) {
                    responseBuilder.status = Status.ERROR
                    responseBuilder.error = responseBuilder.error.toBuilder()
                            .setCode(e.errorCode)
                            .setMessage(e.message)
                            .build()
                } finally {
                    responseObserver?.onNext(responseBuilder.build())
                    responseObserver?.onCompleted()
                }
            }
        }
    }

    private fun initializeDatabase() {
        val url = "jdbc:postgresql://localhost:5432/<your_database>"
        val driver = "org.postgresql.Driver"
        val user = "<your_user>"
        val password = "<your_password>"

        Database.connect(url, driver, user, password)

        transaction {
            SchemaUtils.create(Fruits)
        }
    }
}

fun main(args: Array<String>) {
    val server = FruitStoreServer(50051)
    server.start()
    server.blockUntilShutdown()
}