import { motion, type Variants } from "framer-motion";
import { hero } from "../content";

const EASE = [0.16, 1, 0.3, 1] as const;

type Props = {
  reducedMotion: boolean;
};

export function HeroContent({ reducedMotion }: Props) {
  const delay = reducedMotion ? 0 : 2.4;

  const container: Variants = {
    hidden: {},
    show: { transition: { staggerChildren: 0.13, delayChildren: delay } },
  };

  const rise: Variants = {
    hidden: reducedMotion
      ? { opacity: 0 }
      : { opacity: 0, y: 26, filter: "blur(12px)" },
    show: {
      opacity: 1,
      y: 0,
      filter: "blur(0px)",
      transition: { duration: reducedMotion ? 0.4 : 1.1, ease: EASE },
    },
  };

  return (
    <div className="pointer-events-none absolute inset-0 z-10 flex flex-col">
      <motion.nav
        initial={{ opacity: 0, y: -12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 1, ease: EASE, delay: reducedMotion ? 0 : 0.4 }}
        className="pointer-events-auto flex items-center justify-between px-6 py-6 sm:px-10"
      >
        <span className="text-lg font-semibold tracking-tight text-white">
          {hero.wordmark}
        </span>
        <div className="hidden items-center gap-8 text-sm text-white/60 sm:flex">
          {hero.nav.map((item) => (
            <a
              key={item.label}
              href={item.href}
              className="transition-colors hover:text-white"
            >
              {item.label}
            </a>
          ))}
        </div>
        <a
          href={hero.primaryCta.href}
          className="rounded-full border border-white/15 bg-white/5 px-4 py-1.5 text-sm font-medium text-white/90 backdrop-blur-sm transition-colors hover:border-white/30 hover:bg-white/10"
        >
          {hero.primaryCta.label}
        </a>
      </motion.nav>

      <motion.div
        variants={container}
        initial="hidden"
        animate="show"
        className="flex flex-1 flex-col items-center justify-center px-6 text-center"
        style={{ transform: "translateY(-6%)" }}
      >
        <motion.span
          variants={rise}
          className="mb-6 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.03] px-4 py-1.5 text-xs font-medium uppercase tracking-[0.2em] text-white/55 backdrop-blur-sm"
        >
          <span className="h-1.5 w-1.5 rounded-full bg-burla-cyan shadow-[0_0_12px_2px_rgba(126,203,221,0.8)]" />
          {hero.eyebrow}
        </motion.span>

        <h1 className="max-w-4xl text-balance text-5xl font-semibold leading-[0.98] tracking-[-0.02em] sm:text-6xl lg:text-7xl xl:text-[5.5rem]">
          <motion.span variants={rise} className="block text-white">
            {hero.titleLines[0]}
          </motion.span>
          <motion.span
            variants={rise}
            className="block bg-gradient-to-b from-burla-glow to-burla-cyan bg-clip-text text-transparent"
          >
            {hero.titleLines[1]}
          </motion.span>
        </h1>

        <motion.p
          variants={rise}
          className="text-shadow-soft mt-7 max-w-xl text-pretty text-base leading-relaxed text-white/70 sm:text-lg"
        >
          {hero.subhead}
        </motion.p>

        <motion.div
          variants={rise}
          className="pointer-events-auto mt-10 flex flex-col items-center gap-4 sm:flex-row"
        >
          <a
            href={hero.primaryCta.href}
            className="group inline-flex items-center gap-2 rounded-full bg-burla-cyan px-6 py-3 text-sm font-semibold text-[#04111a] shadow-[0_0_40px_-6px_rgba(126,203,221,0.7)] transition-all hover:shadow-[0_0_56px_-2px_rgba(126,203,221,0.95)]"
          >
            {hero.primaryCta.label}
            <span className="transition-transform group-hover:translate-x-0.5">
              &rarr;
            </span>
          </a>
          <a
            href={hero.secondaryCta.href}
            className="inline-flex items-center rounded-full border border-white/15 bg-black/25 px-6 py-3 text-sm font-medium text-white/85 backdrop-blur-sm transition-colors hover:border-white/30 hover:text-white"
          >
            {hero.secondaryCta.label}
          </a>
        </motion.div>

        <motion.div
          variants={rise}
          className="mt-10 inline-flex items-center gap-2 rounded-lg border border-white/10 bg-black/30 px-4 py-2 font-mono text-sm text-white/70 backdrop-blur-sm"
        >
          <span className="select-none text-burla-cyan/80">&gt;&gt;&gt;</span>
          <code>{hero.code}</code>
        </motion.div>
      </motion.div>
    </div>
  );
}
