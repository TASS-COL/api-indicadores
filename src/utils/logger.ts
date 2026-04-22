export function ts() {
  const d = new Date();
  return (
    d.toLocaleTimeString("es-CO", { hour12: false }) +
    "." +
    String(d.getMilliseconds()).padStart(3, "0")
  );
}
export const logger = {
  info: (...a: any[]) => console.log(`[${ts()}]`, ...a),
  warn: (...a: any[]) => console.warn(`[${ts()}]`, ...a),
  error: (...a: any[]) => console.error(`[${ts()}]`, ...a),
  debug: (...a: any[]) =>
    process.env.DEBUG ? console.log(`[${ts()}][debug]`, ...a) : void 0,
};
