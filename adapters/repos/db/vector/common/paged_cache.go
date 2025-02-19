package common

type PagedCache[T any] struct {
	cache     [][]*T
	pageSize  int
	freePages [][]*T
}

func NewPagedCache[T any](pageSize int) *PagedCache[T] {
	return NewPagedCacheWith[T](pageSize, 10)
}

func NewPagedCacheWith[T any](pageSize int, initalPages int) *PagedCache[T] {
	return &PagedCache[T]{
		pageSize: pageSize,
		cache:    make([][]*T, 10),
	}
}

func (p *PagedCache[T]) Get(id int) *T {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if p.cache[pageID] == nil {
		return nil
	}

	return p.cache[pageID][slotID]
}

func (p *PagedCache[T]) Set(id int, value *T) {
	pageID := id / p.pageSize
	slotID := id % p.pageSize

	if pageID >= len(p.cache) {
		p.grow(pageID)
	}

	if p.cache[pageID] == nil {
		p.cache[pageID] = p.getPage()
	}

	p.cache[pageID][slotID] = value
}

func (p *PagedCache[T]) grow(page int) {
	newSize := max(page+10, len(p.cache)*2)
	newCache := make([][]*T, newSize)
	copy(newCache, p.cache)
	p.cache = newCache
}

func (p *PagedCache[T]) getPage() []*T {
	if len(p.freePages) > 0 {
		lastIndex := len(p.freePages) - 1
		page := p.freePages[lastIndex]
		p.freePages = p.freePages[:lastIndex]
		return page
	}

	return make([]*T, p.pageSize)
}

func (p *PagedCache[T]) Reset() {
	for i := range p.cache {
		if p.cache[i] != nil {
			clear(p.cache[i])
			p.freePages = append(p.freePages, p.cache[i])
			p.cache[i] = nil
		}
	}
}

func (p *PagedCache[T]) Cap() int {
	return len(p.cache) * p.pageSize
}
