// tvShowMicroservice.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
// Charger le fichier tvShow.proto
const tvShowProtoPath = 'tvShow.proto';
const mongoose = require('mongoose');
const TvShows = require('./models/tvShowsModel');
const { Kafka } = require('kafkajs');

const tvShowProtoDefinition = protoLoader.loadSync(tvShowProtoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

const tvShowProto = grpc.loadPackageDefinition(tvShowProtoDefinition).tvShow;
const url = 'mongodb://localhost:27017/tvShows';
//const dbName = 'moviesDB';

mongoose.connect(url)
    .then(() => {
        console.log('connected to database!');
    }).catch((err) => {
        console.log(err);
    })

/*const tv_shows = [
    {
        id: '1',
        title: 'Exemple de série TV 1',
        description: 'Ceci est le premier exemple de série TV.',
    },
    {
        id: '2',
        title: 'Exemple de série TV 2',
        description: 'Ceci est le deuxième exemple de série TV.',
    },
];*/

const tvShowService = {
    getTvshow: async (call, callback) => {
        try {
            const tvShowId = call.request.tv_show_id;
            //console.log(call.request);
            const tvShow = await TvShows.findOne({ _id: tvShowId }).exec();
            await producer.connect();
            await producer.send({
                topic: 'tv-shows-topic',
                messages: [{ value: 'Searched for TV show id : '+tvShowId.toString() }],
            });

            if (!tvShow) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Tv Show not found' });
                return;
            }
            callback(null, { tv_show: tvShow });

        } catch (error) {
            await producer.connect();
            await producer.send({
                topic: 'tv-shows-topic',
                messages: [{ value: `Error occurred while fetching tv shows: ${error}` }],
            });
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching tv show' });
        }

        //const tv_show=getTvshowById(tvShowId);
        //console.log(tv_show);
        // if (!tv_show) {
        // callback({ code: grpc.status.NOT_FOUND, message: 'Tv Show not found' });
        //  return;
        // }
        /*const tv_show = {
            id: call.request.tv_show_id,
            title: 'Exemple de série TV',
            description: 'Ceci est un exemple de série TV.',
        };*/
        //callback(null, { tv_show });
    },
    searchTvshows: async (call, callback) => {
        try {
            const tvShows = await TvShows.find({}).exec();

            await producer.connect();
            await producer.send({
                topic: 'tv-shows-topic',
                messages: [{ value: 'Searched for TV shows' }],
            });

            callback(null, { tv_shows: tvShows });
        } catch (error) {
            await producer.connect();
            await producer.send({
                topic: 'tv-shows-topic',
                messages: [{ value: `Error occurred while fetching tv shows: ${error}` }],
            });

            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching tv shows' });
        }
    },
    
    addTvShow: async (call, callback) => {
        /*const id=tv_shows.length+1;
        const titre = call.request.title;
        const desc = call.request.description;
        const newTvShow ={
            id:id,
            title:titre,
            description:desc
        }
        console.log(newTvShow);
        newTvShow.id=id;
        tv_shows.push(newTvShow);
        callback(null, { tv_show:newTvShow});*/
        /*const { title, description } = call.request;
        console.log(call.request);
        const newTvShow = new TvShows({ title, description });
        await newTvShow.save()
            .then(savedTvShow => {
                producer.connect();
                producer.send({
                    topic: 'tv-shows-topic',
                    messages: [{ value: JSON.stringify(newTvShow) }],
                });
                //producer.disconnect();
                callback(null, { tv_show: savedTvShow });
            })
            .catch(error => {
                callback({ code: grpc.status.INTERNAL, message: 'Error occurred while adding tv show' });
            });*/
        const { title, description } = call.request;
        console.log(call.request);
        const newTvShow = new TvShows({ title, description });

        try {
            await producer.connect();

            await producer.send({
                topic: 'tv-shows-topic',
                messages: [{ value: JSON.stringify(newTvShow) }],
            });

            await producer.disconnect();

            const savedTvShow = await newTvShow.save();

            callback(null, { tv_show: savedTvShow });
        } catch (error) {
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while adding tv show' });
        }
    }


};

function getTvshowById(tvShowId) {

    return tv_shows.find(tv_show => tv_show.id === tvShowId);
}

// Créer et démarrer le serveur gRPC
const server = new grpc.Server();
server.addService(tvShowProto.TVShowService.service, tvShowService);
const port = 50052;
server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(),
    (err, port) => {
        if (err) {
            console.error('Échec de la liaison du serveur:', err);
            return;
        }
        console.log(`Le serveur s'exécute sur le port ${port}`);
        server.start();
    });
console.log(`Microservice de séries TV en cours d'exécution sur le port
${port}`);