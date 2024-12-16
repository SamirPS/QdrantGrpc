package io.gatling.grpc.demo;


import io.gatling.javaapi.core.*;
import io.gatling.javaapi.grpc.*;
import qdrant.*;
import qdrant.Collections;

import java.util.*;


import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.grpc.GrpcDsl.*;

public class QdrantCollectionSimulation extends Simulation {

    GrpcProtocolBuilder baseGrpcProtocol = grpc.forAddress("cf9b7660-8d9e-4af0-ab5a-ecef8e364e4e.eu-west-1-0.aws.cloud.qdrant.io", 6334)
            .asciiHeader("api-key").value("5y4rZlQB9vNRP5_aJ5Mptv6i7TKVD3KJMcJmZ8JqExDm82TgEo9PWg");


    ScenarioBuilder GetCollections = scenario("LoadTest Collections")
            .exec(session -> {
                Collections.Distance distanceofcollection =  Collections.Distance.forNumber((int)(Math.random() * 4) + 1);
                int sizeofcollection =  (int)(Math.random() * 65536);
                String collectionName = "test" + UUID.randomUUID().toString();
                session = session.set("collectionName", collectionName);
                session = session.set("distanceofcollection", distanceofcollection);
                session = session.set("sizeofcollection", sizeofcollection);
                return session;
            })
            .exec(
                    grpc("Create a collection")
                            .unary(CollectionsGrpc.getCreateMethod())
                            .send(session -> Collections.CreateCollection.newBuilder()
                                    .setCollectionName(session.getString("collectionName"))
                                    .setVectorsConfig(Collections.VectorsConfig.newBuilder().
                                            setParams(Collections.VectorParams.newBuilder()
                                                    .setSize(session.getInt("sizeofcollection"))
                                                    .setOnDisk(true)
                                                    .setDistance(session.get("distanceofcollection"))
                                                    .build())
                                            .build())
                                    .build())
                            .check(
                                    response(Collections.CollectionOperationResponse::getResult).is(true)
                            )
            )
            .exec(
                    grpc("Add points")
                            .unary(PointsGrpc.getUpsertMethod())
                            .send(session -> {
                                List<Points.PointStruct> pointsList = new ArrayList<>();
                                int vectorSize = session.getInt("sizeofcollection");
                                List<String> cities = Arrays.asList(
                                        "Paris", "Lyon", "Marseille", "Bordeaux",
                                        "Toulouse", "Nantes", "Strasbourg", "Lille",
                                        "Nice", "Rennes", "Montpellier", "Grenoble"
                                );

                                for (int i = 1; i <= 5; i++) {
                                    List<Float> vectorValues = new ArrayList<>();
                                    for (int j = 0; j < vectorSize; j++) {
                                        vectorValues.add((float) Math.random());
                                    }

                                    Random random = new Random();
                                    String randomCity = cities.get(random.nextInt(cities.size()));
                                    Points.PointStruct point = Points.PointStruct.newBuilder()
                                            .setId(Points.PointId.newBuilder()
                                                    .setNum(i)
                                                    .build())
                                            .setVectors(Points.Vectors.newBuilder()
                                                    .setVector(Points.Vector.newBuilder()
                                                            .addAllData(vectorValues)
                                                            .build())
                                                    .build())
                                            .putPayload("city", JsonWithInt.Value.newBuilder()
                                                    .setStringValue(randomCity).build())
                                            .build();

                                    pointsList.add(point);
                                }

                                return Points.UpsertPoints.newBuilder()
                                        .setCollectionName(session.getString("collectionName"))
                                        .setWait(true)
                                        .addAllPoints(pointsList)
                                        .build();
                            })
                            .check(
                                    response(Points.PointsOperationResponse::getResult).transform(Points.UpdateResult::getStatus)
                                            .is(Points.UpdateStatus.Completed)
                            )
            )
            .exec(
                    grpc("Query points with filter")
                            .unary(PointsGrpc.getQueryMethod())
                            .send(session -> Points.QueryPoints.newBuilder()
                                    .setCollectionName(session.getString("collectionName"))
                                    .setLimit(1)
                                    .build())
                            .check(
                                    response(Points.QueryResponse::getResultList)
                                            .transform(res -> res.get(0).getId())
                                            .is(Points.PointId.newBuilder()
                                                    .setNum(1)
                                                    .build())
                            )
            )
    .exec(
            grpc("Create the snapshot")
                            .unary(SnapshotsGrpc.getCreateMethod())
            .send(session -> SnapshotsService.CreateSnapshotRequest.newBuilder()
            .setCollectionName(session.getString("collectionName"))
                                    .build())
            .check(
            response(SnapshotsService.CreateSnapshotResponse::getSnapshotDescription)
            )

            )
            .exec(
                    grpc("List the snapshot")
                            .unary(SnapshotsGrpc.getListMethod())
                            .send(session -> SnapshotsService.ListSnapshotsRequest.newBuilder()
                                    .setCollectionName(session.getString("collectionName"))
                                    .build())
                            .check(
                                    response(SnapshotsService.ListSnapshotsResponse::getSnapshotDescriptionsList)
                                            .transform(
                                                    res -> res.size()
                                            )
                                            .is(1)
                            )
            )
            .exec(
                    grpc("Count the numbers of Points")
                            .unary(PointsGrpc.getCountMethod())
                            .send(session -> Points.CountPoints.newBuilder()
                                    .setCollectionName(session.getString("collectionName"))
                                    .setExact(true)
                                    .build())
                    .check(
                            response(Points.CountResponse::getResult)
                                    .transform(res -> res.getCount())
                                    .is(5L)
                    )
            );

    // ./mvnw gatling:test -Dgatling.simulationClass=io.gatling.grpc.demo.GreetingSimulation
    // ./mvnw gatling:test -Dgrpc.scenario=deadlines -Dgatling.simulationClass=io.gatling.grpc.demo.GreetingSimulation

    {
        setUp(GetCollections.injectOpen(rampUsers(1).during(20))).protocols(baseGrpcProtocol);
    }
}
