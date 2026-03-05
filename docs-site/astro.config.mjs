// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'klite',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/klaudworks/klite' }],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Quickstart', slug: 'guides/getting-started' },
					],
				},
			{
				label: 'Deployment',
				items: [
					{ label: 'Configuration', slug: 'guides/configuration' },
					{ label: 'Kubernetes & Helm', slug: 'guides/kubernetes' },
				],
			},
				{
					label: 'Concepts',
					autogenerate: { directory: 'concepts' },
				},
			{
				label: 'Guides',
				items: [
					{ label: 'Migrating from Kafka', slug: 'guides/migrating-from-kafka' },
					{ label: 'Troubleshooting', slug: 'guides/troubleshooting' },
				],
			},
			{
				label: 'Reference',
				autogenerate: { directory: 'reference' },
			},
			{
				label: 'Performance',
				items: [
					{ label: 'Benchmarks', slug: 'reference/benchmarks' },
				],
			},
			],
		}),
	],
});
