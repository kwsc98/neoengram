import { render, screen } from '@testing-library/vue';
import ElementPlus from 'element-plus';
import { describe, expect, it } from 'vitest';

import JobStateTag from '@/components/JobStateTag.vue';

describe('JobStateTag', () => {
  it('renders the Chinese operational label', () => {
    render(JobStateTag, { props: { state: 'prepared' }, global: { plugins: [ElementPlus] } });
    expect(screen.getByText('待发布')).toBeInTheDocument();
  });
});
