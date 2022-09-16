package raft

import (
	"fmt"
)

type Entry struct {
	Command interface{}
	Term int
	Index int
}

type Log struct {
	Entries []Entry
	Index int
}

// 添加日志
func (l *Log) append(entries ...Entry) {
	l.Entries = append(l.Entries,entries...)
}

// 创建空Log
func (l *Log) makeEmptyLog() Log{
	var log = Log{
		Entries: make([]Entry,0),
		Index: 0,
	}
	return log
}

// 寻找第i条日志
func (l *Log) at(id int) *Entry{
	return &l.Entries[id]
}


// 计算Log长度
func (l *Log)len() int{
	return len(l.Entries)
}

// 找到最后一条日志
func (l *Log) lastLog() *Entry{
	return l.at(l.len() - 1)
}

// 返回 Entry 中的 Term
func (e *Entry) getEntryTerm() string{
	return fmt.Sprintln(e.Term)
}

// 截断 Entries 至 id
func (l *Log) truncate(id int) {
	l.Entries = l.Entries[:id]
}

// 获取从 id 开始的 Entries
func (l *Log) slice(id int) []Entry{
	return l.Entries[id:]
}


