import os
import random
from logging.config import dictConfig
from flask import Flask, jsonify, request
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool
from datetime import datetime, timedelta

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

RATELIMIT_STORAGE_URI = os.environ.get("RATELIMIT_STORAGE_URI", "memory://")

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger

# Use the DATABASE_URL environment variable if it exists, otherwise use the default.
# Use the format postgres://username:password@hostname/database_name to connect to the database.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@postgres/postgres")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": False, 
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)


@app.route("/", methods=("GET",))
def lista_aeroportos():
    """Mostra todos os aeroportos por ordem alfabética do aeroporto"""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            aeroportos = cur.execute(
                """
                SELECT nome, cidade
                FROM aeroporto
                ORDER BY nome ASC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    if len(aeroportos) == 0:
        return jsonify({"message": "Não existe nenhum aeroporto."}), 200
        
    aeroportos_dict = [{"nome": aeroporto[0], "cidade": aeroporto[1]} for aeroporto in aeroportos]
    return jsonify(aeroportos_dict), 200

@app.route("/voos/<partida>/", methods=("GET",))
def proximas_partidas(partida):
    """Mostra os voos que vão acontecer nas proximas
    12 horas a partir do aeroporto de partida"""
    agora = datetime.now()
    final = agora + timedelta(hours=12)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            
            cur.execute("SELECT 1 FROM aeroporto WHERE codigo = %s", (partida,))
            if cur.fetchone() is None:
                return jsonify({"message": f"Aeroporto {partida} nao encontrado.", "status": "error", "code":404}), 404
            
            partidas = cur.execute(
                """
                SELECT no_serie, hora_partida, nome, chegada
                FROM voo v JOIN aeroporto a ON v.chegada = a.codigo
                WHERE partida = %(partida)s AND
                hora_partida BETWEEN %(agora)s AND %(final)s;
                """,
                {"partida": partida, "agora": agora, "final": final},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    if len(partidas) == 0:
        return jsonify({"message": f"Nao ha partidas do aeroporto {partida} nas proximas 12 horas."}), 200

    partidas_dict = [{"no_serie": partida[0], "hora de partida": partida[1], 
                      "aeroporto de chegada": partida[2], "codigo": partida[3]} for partida in partidas]
    return jsonify(partidas_dict), 200

@app.route("/voos/<partida>/<chegada>/", methods=("GET",))
def proximos_voos_rota(partida, chegada):
    """Mostra os 3 próximos voos entre os aeroportos de chegada e partida que tenham bilhetes disponiveis"""
    agora = datetime.now()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            
            cur.execute("SELECT 1 FROM aeroporto WHERE codigo = %s", (partida,))
            if cur.fetchone() is None:
                return jsonify({"message": f"Aeroporto de partida {partida} nao encontrado.", "status": "error", "code":404}), 404
            
            cur.execute("SELECT 1 FROM aeroporto WHERE codigo = %s", (chegada,))
            if cur.fetchone() is None:
                return jsonify({"message": f"Aeroporto de chegada {chegada} nao encontrado.", "status": "error", "code":404}), 404
            
            
            voos = cur.execute(
                """
                SELECT v.no_serie, v.hora_partida
                FROM voo v
                WHERE (
                    SELECT COUNT(*) 
                    FROM assento a 
                    WHERE a.no_serie = v.no_serie
                    ) - (
                    SELECT COUNT(*) 
                    FROM bilhete b 
                    WHERE b.voo_id = v.id
                    ) > 0 
                    AND partida = %(partida)s 
                    AND chegada = %(chegada)s
                    AND hora_partida >= %(agora)s
                GROUP BY v.no_serie, v.hora_partida
                ORDER BY v.hora_partida
                LIMIT 3;
                """,
                {"partida": partida, "chegada": chegada, "agora": agora},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    if len(voos) == 0:
        return jsonify({"message": f"Nao ha voos entre os aeroportos {partida} e {chegada}."}), 200

    voos_dict = [{"no_serie": voo[0], "hora de partida": voo[1]} for voo in voos]
    return jsonify(voos_dict), 200


@app.route("/compra/<voo>/", methods=("POST",))
def buys_ticket(voo):
    """Faz a compra de um ou mais bilhetes"""

    data = request.get_json()
    nif = data.get("nif")
    bilhetes = data.get("bilhetes")

    if nif is None or not bilhetes:
        return jsonify({"message": "Dados em falta.", "status": "error", "code":400}), 400

    classes = []
    bilhete_1c = 0
    bilhete_2c = 0

    for bilhete in bilhetes:
        if bilhete["prim_classe"] not in classes:
            classes.append(bilhete["prim_classe"])
        if bilhete["prim_classe"]:
            bilhete_1c += 1
        else:
            bilhete_2c += 1

    with pool.connection() as conn:
        with conn.cursor() as cur:
            hora_partida = cur.execute(
                """
                SELECT hora_partida FROM voo WHERE id = %(voo_id)s;
                """,
                {"voo_id": voo}
            ).fetchone()[0]

            if hora_partida < datetime.now():
                return jsonify({"message": f"O voo {voo} ja partiu.", "status": "error", "code":409}), 409

            no_serie = cur.execute(
                """
                SELECT no_serie FROM voo v WHERE v.id = %(voo_id)s;
                """,
                {"voo_id": voo},
            ).fetchone()

            if no_serie is None:
                return jsonify({"message": f"Voo {voo} nao existe.", "status": "error", "code":404}), 404

            no_serie = no_serie[0]

            id_reserva = cur.execute(
                """
                INSERT INTO venda (nif_cliente, balcao, hora)
                VALUES (%(nif_cliente)s, NULL, %(hora)s)
                RETURNING codigo_reserva;
                """,
                {"nif_cliente": nif, "hora": datetime.now()},
            ).fetchone()[0]

            for prim_classe in classes:
                todos_lugares = cur.execute(
                    """
                    SELECT COUNT(*) FROM assento
                    WHERE no_serie = %(no_serie)s AND prim_classe = %(prim_classe)s;
                    """,
                    {"no_serie": no_serie, "prim_classe": prim_classe},
                ).fetchone()[0]

                lugares_vendidos = cur.execute(
                    """
                    SELECT COUNT(*) FROM bilhete b 
                    WHERE b.voo_id = %(voo_id)s AND b.prim_classe = %(prim_classe)s;
                    """,
                    {"voo_id": voo, "prim_classe": prim_classe},
                ).fetchone()[0]

                if prim_classe:
                    if todos_lugares - lugares_vendidos < bilhete_1c:
                        return jsonify({"message": "Sem lugares suficientes na primeira classe.",
                                        "status": "error", "code":409}), 409
                else:
                    if todos_lugares - lugares_vendidos < bilhete_2c:
                        return jsonify({"message": "Sem lugares suficientes na segunda classe.",
                                        "status": "error", "code":409}), 409

            bilhetes_comprados = []
            for bilhete in bilhetes:
                nome = bilhete["nome_passegeiro"]  # erro ortográfico na tabela
                prim_classe = bilhete["prim_classe"]

                if prim_classe:
                    preco = round(random.uniform(500, 2000), 2)
                else:
                    preco = round(random.uniform(50, 500), 2)

                cur.execute(
                    """
                    INSERT INTO bilhete (voo_id, codigo_reserva, nome_passegeiro,
                                         preco, prim_classe)
                    VALUES (%(voo_id)s, %(codigo_reserva)s, %(nome_passageiro)s,
                            %(preco)s, %(classe)s);
                    """,
                    {"voo_id": voo, "codigo_reserva": id_reserva, "nome_passageiro": nome, "preco": preco,
                     "classe": prim_classe},
                )

                if prim_classe:
                    bilhetes_comprados.append({"nome": nome, "classe": "1º classe", "preco": preco})
                else:
                    bilhetes_comprados.append({"nome": nome, "classe": "2º classe", "preco": preco})

            conn.commit()
    return jsonify({"message": "Bilhetes comprados com sucesso.",
                    "codigo_reserva": id_reserva, "bilhetes": bilhetes_comprados}), 201


@app.route("/checkin/<bilhete>/", methods=("POST",))
def checks_in(bilhete):
    """Faz check-in automático de um bilhete, atribuindo um lugar livre da classe correspondente."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            bilhete_id = bilhete
            bilhete = cur.execute(
                """
                SELECT voo_id, prim_classe, lugar, no_serie
                FROM bilhete
                WHERE id = %(id_bilhete)s;
                """,
                {"id_bilhete": bilhete},
            ).fetchone()

            if bilhete is None:
                return jsonify({"message": f"Bilhete {bilhete_id} invalido: o bilhete nao existe.",
                                "status": "error", "code":404}), 404

            voo_id, prim_classe, lugar, no_serie = bilhete

            if lugar is not None and no_serie is not None:
                return jsonify({"message": f"Acao invalida: ja foi realizado o check-in ao bilhete {bilhete_id}.",
                                "status": "error", "code":400}), 400
                
            hora_partida = cur.execute(
                """
                SELECT hora_partida FROM voo WHERE id = %(voo_id)s;
                """,
                {"voo_id": voo_id}
            ).fetchone()[0]

            if hora_partida < datetime.now():
                return jsonify({"message": f"O voo {voo_id} do bilhete {bilhete_id} ja partiu.", 
                                "status": "error", "code":409}), 409

            no_serie = cur.execute(
                """
                SELECT no_serie FROM voo WHERE id = %(voo_id)s;
                """,
                {"voo_id": voo_id},
            ).fetchone()[0]

            lugar = cur.execute(
                """
                SELECT lugar FROM assento WHERE no_serie = %(no_serie)s
                AND prim_classe = %(prim_classe)s AND (lugar, %(voo_id)s) NOT IN (
                    SELECT lugar, voo_id
                    FROM bilhete
                    WHERE voo_id = %(voo_id)s AND lugar IS NOT NULL
                )
                ORDER BY lugar ASC
                LIMIT 1
                FOR UPDATE;
                """,
                {"no_serie": no_serie, "prim_classe": prim_classe, "voo_id": voo_id},
            ).fetchone()

            if lugar is None:
                return jsonify({"message": f"Sem lugares de {'1º classe' if prim_classe else '2º classe'} disponiveis.","status":"error", "code":409}), 409

            lugar = lugar[0]

            cur.execute(
                """
                UPDATE bilhete SET lugar = %(lugar)s, no_serie = %(no_serie)s WHERE id = %(bilhete_id)s;
                """,
                {"lugar": lugar, "no_serie":no_serie, "bilhete_id": bilhete_id},
            )

            conn.commit()

    return jsonify({"message": "Check-in realizado com sucesso.", "lugar": lugar, "no_serie": no_serie}), 201


if __name__ == "__main__":
    app.run()
