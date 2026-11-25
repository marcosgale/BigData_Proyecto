# DECISION_S20: ¿batch o streaming?

Equipo: Alejandro Nieto Aguilar y Marcos García León.

Hechos sacados de `decision/casos.yaml`: 12 aulas, unos 180 eventos por hora en día lectivo. Los dos casos usan los mismos eventos; lo que cambia es quién los consume y cuándo.

## Caso A · Informe de las ocho

```text
latencia_util: horas; el informe se mira una vez al día a las 08:00 y basta con que esté listo antes de esa hora
frescura_admitida: datos hasta la noche anterior; lo que pase entre las 06:00 y las 08:00 puede esperar al día siguiente
decision: un job batch diario en la ventana 02:00–06:00 que lee los eventos del día anterior y escribe el informe
consumidor_continuo: no; hay una única lectura al día, a las 08:00, y nadie mira el resultado el resto del tiempo
conclusion_arquitectura: batch (un job nocturno, como spark/job_01.py, que lee, calcula y termina)
por_que_no_la_otra: streaming 24/7 tendría Kafka y un query encendidos todo el día, con checkpoint y estado que vigilar, para un resultado que alguien lee una sola vez a las 08:00; con ~180 eventos/hora un job nocturno lo procesa en minutos y si falla hay horas para relanzarlo antes de las 08:00
```

- **Quién y cuándo:** el responsable de ocupación abre el informe a las 08:00, una vez al día.
- **Si llega tarde:** si no está a las 08:00, ese día se decide sin informe. Por eso el job corre de madrugada y deja margen de horas para relanzarlo si falla.
- **Si llega antes:** ninguna acción; da igual que esté a las 04:00 o a las 07:55, nadie lo mira hasta las 08:00.

## Caso B · Alerta de aforo

```text
latencia_util: menos de 2 minutos desde que el aula pasa el aforo hasta que salta el aviso
frescura_admitida: minutos; un dato de hace una hora ya no sirve para echar a nadie de un aula llena
decision: query de Spark Structured Streaming leyendo de Kafka, ventanas de 1 minuto por aula y watermark de pocos minutos, con su propio checkpoint, que avisa cuando la cuenta pasa el aforo
consumidor_continuo: sí; conserjería tiene que poder recibir el aviso en cualquier momento de la jornada
conclusion_arquitectura: streaming (un query que no termina, como temp-stream de la sesión 19)
por_que_no_la_otra: un batch nocturno avisaría al día siguiente, horas tarde, cuando el aula ya se vació; y un batch cada 2 minutos sería relanzar un job sin parar, que es reinventar el micro-lote del streaming pero sin estado ni checkpoint
```

- **Quién y cuándo:** conserjería, en el momento en que un aula pasa el aforo, a cualquier hora lectiva.
- **Si llega tarde:** pasados unos minutos el aviso ya no sirve; el aula sigue llena y el riesgo ya ha ocurrido sin que nadie actúe.
- **Si llega antes:** si avisara antes de pasar el aforo (por ejemplo al 90 %), conserjería podría cortar la entrada antes de llenarse; sería un umbral de preaviso, no otra arquitectura.

## Resumen

Los mismos eventos de las mismas aulas dan dos respuestas distintas porque cambia el consumidor. Si se lee una vez al día y admite horas de retraso, batch. Si alguien tiene que reaccionar en menos de 2 minutos en cualquier momento, streaming. Que ya tengamos Kafka montado de la sesión 19 no es motivo para usarlo en el caso A.
