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
						{ label: 'Docker', slug: 'guides/docker' },
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
						{ label: 'Consumer Groups', slug: 'guides/consumer-groups' },
						{ label: 'Migrating from Kafka', slug: 'guides/migrating-from-kafka' },
						{ label: 'Monitoring', slug: 'guides/monitoring' },
						{ label: 'Troubleshooting', slug: 'guides/troubleshooting' },
					],
				},
				{
					label: 'Reference',
					autogenerate: { directory: 'reference' },
				},
			],
		}),
	],
});
