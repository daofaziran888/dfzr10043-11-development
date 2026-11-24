<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "流程模板", "created": 0, "pending": 0, "abnormal": 0}, {"name": "元素定位", "created": 0, "pending": 0, "abnormal": 0}, {"name": "机器人节点", "created": 0, "pending": 0, "abnormal": 0}, {"name": "流程执行", "created": 0, "pending": 0, "abnormal": 0}, {"name": "步骤轨迹", "created": 0, "pending": 0, "abnormal": 0}, {"name": "任务队列", "created": 0, "pending": 0, "abnormal": 0}, {"name": "凭据保管", "created": 0, "pending": 0, "abnormal": 0}, {"name": "表单字段", "created": 0, "pending": 0, "abnormal": 0}, {"name": "文件归集", "created": 0, "pending": 0, "abnormal": 0}, {"name": "识别区域", "created": 0, "pending": 0, "abnormal": 0}, {"name": "排程计划", "created": 0, "pending": 0, "abnormal": 0}, {"name": "异常规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "重试策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集批次", "created": 0, "pending": 0, "abnormal": 0}, {"name": "字段映射", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据落库", "created": 0, "pending": 0, "abnormal": 0}, {"name": "审计轨迹", "created": 0, "pending": 0, "abnormal": 0}, {"name": "资源额度", "created": 0, "pending": 0, "abnormal": 0}, {"name": "版本发布", "created": 0, "pending": 0, "abnormal": 0}, {"name": "运行指标", "created": 0, "pending": 0, "abnormal": 0}, {"name": "抽检样本", "created": 0, "pending": 0, "abnormal": 0}, {"name": "交接包", "created": 0, "pending": 0, "abnormal": 0}, {"name": "补采清单", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
