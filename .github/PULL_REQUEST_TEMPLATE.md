## Qué cambia

<!-- Resumen del cambio en dos o tres frases. Si cierra un issue: Closes #n -->

## Por qué

<!-- El motivo: qué problema resuelve o qué parte del pipeline añade -->

## Cómo se ha probado

- [ ] `ruff check .` sin errores
- [ ] `python -m pytest tests/02_silver -q` en verde
- [ ] `python -m pytest tests/03_gold -q` en verde (si cambia Silver o Gold)
- [ ] `docker compose config` sin avisos (si cambia `compose.yaml` o `.env.example`)
- [ ] Ejecutado contra el stack real: DAG / job / script y resultado observado

## Después de mezclar

<!-- Qué tiene que hacer el otro miembro al actualizar: reconstruir una imagen, reejecutar un DAG, borrar un volumen, cambiar el .env... Si nada, decirlo -->

## Documentación

- [ ] README, `docs/` o las cards actualizados si el cambio lo requiere
- [ ] `CHANGELOG.md` actualizado
