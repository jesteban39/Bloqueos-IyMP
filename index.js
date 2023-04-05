const pg = require('pg');
const ibmdb = require('ibm_db');
const fs = require('fs');
const dotenv = require('dotenv');

dotenv.config();

// constantes para conectarse a las BD
const configSVD =`DATABASE=${process.env.SVD_DATABASE};HOSTNAME=${process.env.SVD_HOSTNAME};UID=${process.env.SVD_UID};PWD=${process.env.SVD_PWD};PORT=${process.env.SVD_PORT};PROTOCOL=TCPIP`;

const configIyMP = {
    user: process.env.IMP_USER,
    host: process.env.IMP_HOST,
    password: process.env.IMP_PASS,
    database: process.env.IMP_NAME,
    port: process.env.IMP_PORT
}


// scrpts SQL para utilizar durante el proceso
const querys = {
    iymp: {
        searchNull: "SELECT DISTINCT bc.cliente as \"cliente\" FROM public.bloqueo_cliente bc WHERE bc.definicion_bloqueo_cliente IS NULL LIMIT $limit;",
        deleteBloqueo: "DELETE FROM public.bloqueo_cliente bc WHERE bc.cliente = '$cliente';",
        deleteAllNull: "DELETE FROM public.bloqueo_cliente bc WHERE bc.definicion_bloqueo_cliente IS NULL;",
        incertBloqueo: "INSERT INTO public.bloqueo_cliente VALUES($fechaFinTeorica,$fechaFinReal,'$cliente',$definicionBloqueo);",
    },
    //AND (bc.FECHA_BLOQUEO >= DATE('$update') OR bc.FECHA_FIN_REAL IS NOT NULL )
    svd: {
        searchBloqueosUpdate: "SELECT DISTINCT bc.CLIENTE AS \"cliente\", bc.LAST_UPDATE AS \"update\" FROM SVD.BLOQUEO_CLIENTE bc WHERE bc.LAST_UPDATE >= '$update' AND (bc.FECHA_BLOQUEO >= DATE('$update') OR bc.FECHA_FIN_REAL IS NOT NULL ) ORDER BY bc.LAST_UPDATE ASC LIMIT $limit;",
        searchDesbloqueos: "SELECT DISTINCT bc.CLIENTE AS \"cliente\" FROM SVD.BLOQUEO_CLIENTE bc WHERE bc.FECHA_FIN_REAL IS NOT NULL;",
        searchBloqueoCliente: "SELECT bc.FECHA_FIN_TEORICA AS \"fechaFinTeorica\", DATE(bc.FECHA_FIN_REAL) AS \"fechaFinReal\", bc.CLIENTE AS \"cliente\", bc.DEFINICION_BLOQUEO_CLIENTE AS \"definicionBloqueo\" FROM SVD.BLOQUEO_CLIENTE bc WHERE bc.CLIENTE = '$cliente';"
    }
}

// ingresar el limite de MEs que se desea actualizar en una ejecucion
const main = async (limit, secons) => {

    let current = JSON.parse(fs.readFileSync('current.json'));

    try {
        console.log('conectando a IyMP...');
        const poolIymp = new pg.Pool(configIyMP);
        console.log('conectando a SVD...');
        const poolSvd = await ibmdb.open(configSVD);

        do {
            const { update } = current;
            console.log((await poolIymp.query(querys.iymp.deleteAllNull)).rowCount, ' null eliminados');
            console.log('consultando ' + limit + ' bloqueos pora actualizar');
            const searchBloqueos = querys.svd.searchBloqueosUpdate
                .replaceAll('$update', update)
                .replace('$limit', limit);
            const clientsList = await poolSvd.query(searchBloqueos);

            if (!clientsList) throw Error('error al consultar clientes');
            if (clientsList.length <= 0) console.log('no hay bloqueos para corregir');

            // Esperar que Kafka actualice el los ultimos bloqueos antes que this
            await new Promise((resolve, _reject) => setTimeout(() => resolve(), 10000));

            // buscar si el ultimo clinete actualizado esta en medio de la lista consultada
            const init = 0; // clientsList.findIndex((c) => c.cliente == current.cliente);

            for (i = init > 0 ? init : 0; i < clientsList.length; i++) {
                const { cliente, update } = clientsList[i];
                current = clientsList[i];
                console.log('ME ', i + 1, '/', clientsList.length, '-', cliente, '>', update);

                // consultar la informacion correcta del bloqueo para una ME
                const searchBloqueoCliente = querys.svd.searchBloqueoCliente.replace('$cliente', cliente);
                const bloqueos = await poolSvd.query(searchBloqueoCliente);
                if (!bloqueos) throw Error('cliente sin bloqueo');

                // eliminar datos errados para esa ME
                const deleteBloqueo = querys.iymp.deleteBloqueo.replace('$cliente', cliente);
                await poolIymp.query(deleteBloqueo);

                for (j = 0; j < bloqueos.length; j++) {

                    const { fechaFinTeorica,
                        fechaFinReal,
                        cliente,
                        definicionBloqueo } = bloqueos[j];

                    if (!cliente) throw Error('bloqueo sin cliente');
                    if (!definicionBloqueo) throw Error('bloqueo sin definicion');

                    // incertar los valores correctos para cada bloqueo de una ME
                    const incertBloqueo = querys.iymp.incertBloqueo
                        .replace('$fechaFinTeorica', fechaFinTeorica ? `'${fechaFinTeorica}'` : null)
                        .replace('$fechaFinReal', fechaFinReal ? `'${fechaFinReal}'` : null)
                        .replace('$cliente', cliente)
                        .replace('$definicionBloqueo', definicionBloqueo)
                    await poolIymp.query(incertBloqueo);
                }
            }

            fs.writeFileSync('./current.json', JSON.stringify(current));
            console.log('continuar en ' + secons + ' segundos...');
            await new Promise((resolve, _reject) => {
                setTimeout(() => {
                    resolve();
                }, clientsList.length < limit ? secons * 1000 : 500);
            });

        } while (true);

    } catch (error) {
        console.error(error);
    } finally {
        console.log(' terminado ');
        fs.writeFileSync('./current.json', JSON.stringify(current));
    }
}

main(10000, 600);
