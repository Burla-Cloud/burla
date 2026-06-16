import { motion, useReducedMotion } from "framer-motion";
import { Scene } from "./Scene";
import { HeroContent } from "./HeroContent";

export function Hero() {
  const reducedMotion = useReducedMotion() ?? false;

  return (
    <section className="relative h-svh w-full overflow-hidden bg-burla-ink text-white">
      <Scene reducedMotion={reducedMotion} />

      {/* Legibility + edge-blend scrims sitting between the canvas and the copy */}
      <div className="pointer-events-none absolute inset-0 z-[5] bg-[radial-gradient(58%_50%_at_50%_38%,rgba(4,7,10,0.72),rgba(4,7,10,0)_70%)]" />
      <div className="pointer-events-none absolute inset-0 z-[5] bg-gradient-to-b from-burla-ink/85 via-transparent to-burla-ink/95" />

      <HeroContent reducedMotion={reducedMotion} />

      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 1.2, delay: reducedMotion ? 0 : 3.6 }}
        className="pointer-events-none absolute inset-x-0 bottom-7 z-10 flex flex-col items-center gap-2 text-white/40"
      >
        <span className="text-[0.7rem] uppercase tracking-[0.3em]">Scroll</span>
        <motion.span
          animate={reducedMotion ? undefined : { y: [0, 7, 0] }}
          transition={{ duration: 2.2, repeat: Infinity, ease: "easeInOut" }}
          className="block h-7 w-px bg-gradient-to-b from-white/50 to-transparent"
        />
      </motion.div>
    </section>
  );
}
