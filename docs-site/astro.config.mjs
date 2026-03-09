// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import astroMermaid from 'astro-mermaid';

// https://astro.build/config
export default defineConfig({
	integrations: [
		astroMermaid(),
		starlight({
			title: 'klite',
			customCss: ['./src/styles/custom.css'],
			expressiveCode: {
				themes: ['solarized-light', 'solarized-dark'],
			},
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
					items: [
						{ label: 'Design Philosophy', slug: 'concepts/design-philosophy' },
						{ label: 'Storage', slug: 'concepts/storage' },
					{ label: 'Durability', slug: 'concepts/durability' },
					{ label: 'Replication', slug: 'concepts/replication' },
				{ label: 'Testing', slug: 'concepts/testing' },
					],
				},
		{
			label: 'Performance',
			items: [
			{ label: 'Latency Benchmark', slug: 'performance/benchmarks' },
			{ label: 'Throughput Benchmark', slug: 'performance/stress-test' },
				{ label: 'Optimizations', slug: 'performance/optimizations' },
			{ label: 'S3 Cost Calculator', slug: 'performance/s3-cost-calculator' },
			],
		},
			{
				label: 'Guides',
				items: [
					{ label: 'Migrate from / to Kafka', slug: 'guides/migrating-from-kafka' },
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
